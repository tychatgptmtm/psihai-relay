// psihai-relay: держит запрос к модели на сервере, даже если телефон отключился.
// Телефон переподключается и дочитывает ответ с того же места.
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 10000;
const TOKEN = process.env.RELAY_TOKEN || '';
const PUBLIC_URL = (process.env.RENDER_EXTERNAL_URL || process.env.PUBLIC_URL || '').replace(/\/$/, '');
const JOB_TTL_MS = 60 * 60 * 1000; // хранить ответ 60 минут
const PING_MS = 15000;             // пинг во время раздумий модели

const jobs = new Map();

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function upstreamUrl(rest) {
  let u = decodeURIComponent(rest).replace(/^\/+/, '');
  u = u.replace(/^(https?):\/+/, '$1://');
  if (!/^https?:\/\//.test(u)) u = 'https://' + u;
  return u;
}

function passHeaders(req) {
  const h = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (['host', 'content-length', 'connection', 'accept-encoding', 'true-client-ip'].includes(kl)) continue;
    if (kl.startsWith('x-forwarded') || kl.startsWith('cf-') || kl.startsWith('rndr-') || kl.startsWith('render-')) continue;
    h[k] = v;
  }
  return h;
}

function baseUrl(req) {
  if (PUBLIC_URL) return PUBLIC_URL;
  const proto = req.headers['x-forwarded-proto'] || 'https';
  return proto + '://' + req.headers.host;
}

function pushLine(job, line) {
  job.lines.push(line);
  for (const l of job.listeners) l();
}

function finish(job) {
  if (job.done) return;
  if (job.partial) { pushLine(job, job.partial); job.partial = ''; }
  job.done = true;
  job.doneAt = Date.now();
  clearInterval(job.pinger);
  for (const l of job.listeners) l();
}

async function runJob(job, url, headers, body) {
  job.pinger = setInterval(() => {
    if (!job.done && Date.now() - job.lastData > PING_MS) { job.lastData = Date.now(); pushLine(job, ': ping'); }
  }, 5000);
  try {
    const r = await fetch(url, { method: 'POST', headers, body });
    job.status = r.status;
    job.ctype = r.headers.get('content-type') || 'text/event-stream';
    for (const l of job.listeners) l();
    const dec = new TextDecoder();
    for await (const chunk of r.body) {
      job.lastData = Date.now();
      job.partial += dec.decode(chunk, { stream: true });
      const parts = job.partial.split('\n');
      job.partial = parts.pop();
      for (const p of parts) pushLine(job, p.replace(/\r$/, ''));
    }
    job.partial += dec.decode();
  } catch (e) {
    if (!job.status) { job.status = 502; job.ctype = 'text/event-stream'; }
    pushLine(job, 'data: ' + JSON.stringify({ error: { message: 'relay: ' + (e && e.message || e) } }));
  }
  finish(job);
}

function attach(req, res, job, from) {
  let sent = Math.max(0, from | 0);
  let headSent = false;
  const flush = () => {
    if (!job.status) return;
    if (!headSent) {
      headSent = true;
      res.writeHead(job.status, {
        'content-type': job.ctype,
        'cache-control': 'no-cache',
        'x-accel-buffering': 'no',
        'x-relay-resume': job.resumeUrl,
      });
    }
    while (sent < job.lines.length) res.write(job.lines[sent++] + '\n');
    if (job.done) { cleanup(); res.end(); }
  };
  const cleanup = () => job.listeners.delete(flush);
  job.listeners.add(flush);
  res.on('close', cleanup); // телефон отключился — модель продолжает работать
  flush();
}

async function proxyPlain(req, res, url, body) {
  try {
    const r = await fetch(url, { method: req.method, headers: passHeaders(req), body: ['GET', 'HEAD'].includes(req.method) ? undefined : body });
    const buf = Buffer.from(await r.arrayBuffer());
    res.writeHead(r.status, { 'content-type': r.headers.get('content-type') || 'application/json' });
    res.end(buf);
  } catch (e) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: { message: 'relay: ' + (e && e.message || e) } }));
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const u = new URL(req.url, 'http://x');
    let path = req.url.split('?')[0];
    if (path === '/' || path === '/health') { res.writeHead(200); return res.end('psihai-relay ok'); }

    if (TOKEN) {
      const pref = '/' + TOKEN;
      if (!path.startsWith(pref + '/')) { res.writeHead(403); return res.end('bad token'); }
      path = path.slice(pref.length);
    }

    const m = path.match(/^\/_job\/([a-f0-9]{64})$/);
    if (m) {
      const job = jobs.get(m[1]);
      if (!job) { res.writeHead(410); return res.end('gone'); }
      return attach(req, res, job, parseInt(u.searchParams.get('line') || '0', 10));
    }

    const qs = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
    const url = upstreamUrl(path) + qs;
    const body = await readBody(req);

    let stream = false;
    if (req.method === 'POST' && body.length) {
      try { stream = JSON.parse(body.toString('utf8')).stream === true; } catch (_) {}
    }
    if (!stream) return proxyPlain(req, res, url, body);

    const headers = passHeaders(req);
    const auth = req.headers['authorization'] || req.headers['x-api-key'] || '';
    const key = crypto.createHash('sha256').update(auth + '\n' + url + '\n').update(body).digest('hex');

    let job = jobs.get(key);
    if (!job || (job.done && job.status >= 400)) {
      job = {
        key, lines: [], partial: '', listeners: new Set(), done: false, status: 0, ctype: '',
        created: Date.now(), lastData: Date.now(),
        resumeUrl: baseUrl(req) + (TOKEN ? '/' + TOKEN : '') + '/_job/' + key,
      };
      jobs.set(key, job);
      runJob(job, url, headers, body);
    }
    attach(req, res, job, 0);
  } catch (e) {
    try { res.writeHead(500); res.end('relay error: ' + e.message); } catch (_) {}
  }
});

setInterval(() => {
  const now = Date.now();
  for (const [k, j] of jobs) {
    if ((j.done && now - j.doneAt > JOB_TTL_MS) || now - j.created > 3 * JOB_TTL_MS) jobs.delete(k);
  }
}, 60000);

server.requestTimeout = 0;
server.headersTimeout = 60000;
server.keepAliveTimeout = 65000;
server.listen(PORT, () => console.log('psihai-relay on ' + PORT));
