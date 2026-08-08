// Log collector + viewer - receives POSTs from Kong's body-capture plugin,
// offloads large payloads to MinIO, writes the rest to the chat_log DB, and
// serves a small HTTP UI to browse logs and download offloaded files.
//
// Routes:
//   GET  /health            -> OK (readiness probe)
//   POST /logs             -> receive a log from the Kong plugin
//   GET  /                  -> HTML: recent logs
//   GET  /log/:id           -> HTML: one log (full bodies + file download links)
//   GET  /api/logs          -> JSON: recent logs (limit, offset)
//   GET  /api/logs/:id      -> JSON: one log row
//   GET  /asset?ref=minio://<bucket>/<key> -> stream a file from MinIO
//
// If LOG_VIEWER_TOKEN is set, GET /log*, /api/logs*, /asset require it via
// Authorization: Bearer <token> or ?token=<token>. Unset = open (dev).
// ponytail: ~250 lines. Runs on node:22-alpine via ConfigMap mount.
import http from 'node:http';
import crypto from 'node:crypto';
import { URL } from 'node:url';
import postgres from 'postgres';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const Minio = require('minio');
const Busboy = require('busboy');

const sql = postgres(process.env.DATABASE_URL);
const VIEWER_TOKEN = process.env.LOG_VIEWER_TOKEN || '';

const minio = new Minio.Client({
  endPoint: process.env.MINIO_ENDPOINT,
  port: Number(process.env.MINIO_PORT || 9000),
  useSSL: false,
  accessKey: process.env.MINIO_ACCESS_KEY,
  secretKey: process.env.MINIO_SECRET_KEY,
});
const BUCKET = process.env.MINIO_BUCKET || 'chat-assets';
try {
  const exists = await minio.bucketExists(BUCKET);
  if (!exists) await minio.makeBucket(BUCKET);
} catch (e) {
  console.error('minio init failed (uploads will fall back to inline):', e.message);
}

await sql`
  CREATE TABLE IF NOT EXISTS chat_logs (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp     timestamptz NOT NULL DEFAULT now(),
    route         text,
    method        text,
    request_body  text,
    response_body text,
    status_code   int,
    latency       int
  )
`;
const jsonbCols = await sql`
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'chat_logs' AND data_type = 'jsonb'
`;
for (const { column_name } of jsonbCols) {
  await sql.unsafe(`ALTER TABLE chat_logs ALTER COLUMN ${column_name} TYPE text USING ${column_name}::text`);
}

const THRESHOLD = 4096;
const MIME_EXT = {
  'image/png': 'png', 'image/jpeg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp',
  'application/pdf': 'pdf', 'audio/mpeg': 'mp3', 'audio/wav': 'wav',
  'text/plain': 'txt', 'application/octet-stream': 'bin',
};
const EXT_MIME = Object.fromEntries(Object.entries(MIME_EXT).map(([m, e]) => [e, m]));
let assetSeq = 0;

// ---- upload helpers (Kong -> log-collector -> MinIO/DB) ----

async function uploadBuffer(buf, contentType, group, filename) {
  const ext = MIME_EXT[contentType] || (filename && filename.split('.').pop()) || 'bin';
  const safe = (filename || `asset-${assetSeq++}`).replace(/[^a-zA-Z0-9._-]/g, '_');
  const key = `${group}/${Date.now()}-${assetSeq++}-${safe}`;
  await minio.putObject(BUCKET, key, buf, buf.length, { 'Content-Type': contentType || 'application/octet-stream' });
  return `minio://${BUCKET}/${key}`;
}

async function uploadDataUri(dataUri, group) {
  const m = dataUri.match(/^data:([^;]+);base64,(.*)$/s);
  if (!m) return dataUri;
  const buf = Buffer.from(m[2], 'base64');
  try { return await uploadBuffer(buf, m[1], group, `inline.${MIME_EXT[m[1]] || 'bin'}`); }
  catch (e) { console.error('minio upload failed, keeping inline:', e.message); return dataUri; }
}

async function offloadDataUris(obj, group) {
  if (obj === null || typeof obj !== 'object') {
    if (typeof obj === 'string' && obj.startsWith('data:') && obj.length > THRESHOLD) {
      return await uploadDataUri(obj, group);
    }
    return obj;
  }
  if (Array.isArray(obj)) {
    for (let i = 0; i < obj.length; i++) obj[i] = await offloadDataUris(obj[i], group);
    return obj;
  }
  for (const k of Object.keys(obj)) obj[k] = await offloadDataUris(obj[k], group);
  return obj;
}

function processMultipart(bodyBuf, contentType, group) {
  return new Promise((resolve) => {
    const bb = Busboy({ headers: { 'content-type': contentType } });
    const fields = {};
    const files = [];
    const uploads = [];
    bb.on('field', (name, val) => { fields[name] = val; });
    bb.on('file', (name, stream, info) => {
      const chunks = [];
      stream.on('data', c => chunks.push(c));
      stream.on('end', () => {
        uploads.push((async () => {
          const buf = Buffer.concat(chunks);
          try {
            const ref = await uploadBuffer(buf, info.mimeType, group, info.filename);
            files.push({ name, ref, filename: info.filename, content_type: info.mimeType, size: buf.length });
          } catch (e) {
            files.push({ name, error: e.message, filename: info.filename, content_type: info.mimeType, size: buf.length });
          }
        })());
      });
    });
    bb.on('finish', async () => {
      await Promise.all(uploads);
      resolve(JSON.stringify({ _multipart: true, fields, files }));
    });
    bb.on('error', (e) => resolve(JSON.stringify({ _multipart: true, error: e.message, fields, files })));
    bb.end(bodyBuf);
  });
}

async function processBody(body, bodyB64, contentType, group) {
  if (!body) return body;
  let raw = bodyB64 ? Buffer.from(body, 'base64') : body;
  if (contentType && contentType.startsWith('multipart/form-data') && Buffer.isBuffer(raw)) {
    return await processMultipart(raw, contentType, group);
  }
  if (typeof raw === 'string') {
    const t = raw.trimStart();
    if (t.startsWith('{') || t.startsWith('[')) {
      try { const p = JSON.parse(raw); await offloadDataUris(p, group); return JSON.stringify(p); }
      catch { return raw; }
    }
    return raw;
  }
  return raw.toString('base64');
}

// ---- viewer helpers (DB -> browser) ----

// Extract all minio:// refs from a body string.
function extractRefs(body) {
  if (!body) return [];
  return [...body.matchAll(/minio:\/\/[^\s"\\,)]+/g)].map(m => m[0]);
}

function contentTypeFor(key) {
  const ext = (key.split('.').pop() || '').toLowerCase();
  return EXT_MIME[ext] || 'application/octet-stream';
}

// Parse "minio://<bucket>/<key>" -> { bucket, key }.
function parseRef(ref) {
  const m = String(ref).match(/^minio:\/\/([^/]+)\/(.+)$/);
  if (!m) return null;
  return { bucket: m[1], key: m[2] };
}

function authorize(req) {
  if (!VIEWER_TOKEN) return true;                          // dev: open
  const u = new URL(req.url, 'http://x');
  const tok = u.searchParams.get('token')
    || (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  return tok === VIEWER_TOKEN;
}

function htmlShell(title, body) {
  return `<!doctype html><html><head><meta charset=utf-8><title>${title}</title>
<style>body{font:14px/1.5 -apple-system,system-ui,sans-serif;margin:2em auto;max-width:1100px;color:#222}
a{color:#2563eb}table{border-collapse:collapse;width:100%}td,th{border:1px solid #e5e7eb;padding:6px 9px;text-align:left;vertical-align:top}
pre{background:#f8fafc;border:1px solid #e5e7eb;padding:10px;overflow:auto;max-height:400px;white-space:pre-wrap;word-break:break-word}
.muted{color:#6b7280}.tag{display:inline-block;background:#eef2ff;color:#3730a3;border-radius:4px;padding:0 6px;font-size:12px;margin-right:4px}
</style></head><body>${body}</body></html>`;
}

async function streamAsset(ref, res) {
  const parsed = parseRef(ref);
  if (!parsed) { res.writeHead(400); res.end('bad ref'); return; }
  try {
    const stat = await minio.statObject(parsed.bucket, parsed.key);
    const ct = (stat.metaData && stat.metaData['content-type']) || contentTypeFor(parsed.key);
    const filename = parsed.key.split('/').pop();
    res.writeHead(200, {
      'Content-Type': ct,
      'Content-Length': stat.size,
      'Content-Disposition': `inline; filename="${filename}"`,
    });
    minio.getObject(parsed.bucket, parsed.key).then(stream => stream.pipe(res))
      .catch(e => { res.writeHead(500); res.end('minio get failed: ' + e.message); });
  } catch (e) {
    res.writeHead(404); res.end('not found: ' + e.message);
  }
}

// ---- server ----

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x');
  const path = u.pathname;

  if (req.method === 'GET' && (path === '/health' || path === '/healthz')) {
    res.writeHead(200); res.end('OK'); return;
  }

  if (req.method === 'POST' && path === '/logs') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const p = JSON.parse(body);
        const group = `chat-assets/${crypto.randomUUID()}`;
        const reqStored = await processBody(p.request?.body, p.request?.body_b64, p.request?.content_type, group);
        const respStored = await processBody(p.response?.body, p.response?.body_b64, p.response?.content_type, group);
        await sql`
          INSERT INTO chat_logs (route, method, request_body, response_body, status_code, latency)
          VALUES (
            ${p.route?.name || null}, ${p.request?.method || null},
            ${reqStored || null}, ${respStored || null},
            ${p.response?.status || null}, ${p.latencies?.request ?? p.latency?.request ?? null}
          )`;
        res.writeHead(200); res.end('OK');
      } catch (err) {
        console.error('Failed to log:', err.message);
        res.writeHead(500); res.end('Error');
      }
    });
    return;
  }

  // Public assets: GET /public/<key> -> stream chat-assets/public/<key> (NO token).
  // Contained to the public/ prefix; private objects still require the token via /asset.
  if (req.method === 'GET' && path.startsWith('/public/')) {
    const key = decodeURIComponent(path.slice('/public/'.length));
    if (!key || key.includes('..') || key.startsWith('/')) { res.writeHead(400); res.end('bad key'); return; }
    return streamAsset(`minio://${BUCKET}/public/${key}`, res);
  }

  // Everything below is the viewer -> requires token if configured.
  if (!authorize(req)) { res.writeHead(401); res.end('unauthorized'); return; }

  // File download: /asset?ref=minio://bucket/key
  if (req.method === 'GET' && path === '/asset') {
    const ref = u.searchParams.get('ref');
    if (!ref) { res.writeHead(400); res.end('missing ?ref='); return; }
    return streamAsset(ref, res);
  }

  // HTML: list recent logs.
  if (req.method === 'GET' && path === '/') {
    const rows = await sql`SELECT id, timestamp, method, status_code, latency,
                                  left(coalesce(request_body,''),120) AS req_preview,
                                  request_body, response_body
                           FROM chat_logs ORDER BY timestamp DESC LIMIT 50`;
    const tok = VIEWER_TOKEN ? `?token=${VIEWER_TOKEN}` : '';
    const rowsHtml = rows.map(r => {
      const refs = [...extractRefs(r.request_body), ...extractRefs(r.response_body)];
      const refTags = refs.map(x => `<span class=tag>📎 ${refs.indexOf(x) + 1}</span>`).join('');
      return `<tr><td><a href="/log/${r.id}${tok}">${r.id}</a></td>
        <td>${new Date(r.timestamp).toISOString().replace('T',' ').slice(0,19)}</td>
        <td>${r.method}</td><td>${r.status_code}</td><td>${r.latency ?? ''}ms</td>
        <td>${refs.length} ${refTags}</td>
        <td><pre>${(r.req_preview || '').replace(/</g,'&lt;')}</pre></td></tr>`;
    }).join('');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(htmlShell('chat logs', `<h1>chat_logs <span class=muted>(${rows.length} recent)</span></h1>
      <table><tr><th>id</th><th>time</th><th>method</th><th>status</th><th>latency</th><th>files</th><th>request preview</th></tr>
      ${rowsHtml}</table>`));
    return;
  }

  // HTML: one log.
  if (req.method === 'GET' && path.startsWith('/log/')) {
    const id = decodeURIComponent(path.slice('/log/'.length));
    const rows = await sql`SELECT * FROM chat_logs WHERE id = ${id} LIMIT 1`;
    if (!rows.length) { res.writeHead(404); res.end('not found'); return; }
    const r = rows[0];
    const tok = VIEWER_TOKEN ? `?token=${VIEWER_TOKEN}` : '';
    const refs = [...new Set([...extractRefs(r.request_body), ...extractRefs(r.response_body)])];
    const fileLinks = refs.map((ref, i) => {
      const p = parseRef(ref);
      const name = p ? p.key.split('/').pop() : ref;
      return `<li><a href="/asset?ref=${encodeURIComponent(ref)}${tok}">📎 ${name}</a> <span class=muted>${ref}</span></li>`;
    }).join('');
    const pretty = (s) => {
      if (!s) return '<span class=muted>(empty)</span>';
      try { return `<pre>${JSON.stringify(JSON.parse(s), null, 2).replace(/</g,'&lt;')}</pre>`; }
      catch { return `<pre>${s.replace(/</g,'&lt;')}</pre>`; }
    };
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(htmlShell('log ' + id, `<p><a href="/${tok}">← back</a></p>
      <h1>${r.method} ${r.status_code} <span class=muted>${r.latency ?? ''}ms</span></h1>
      <p class=muted>id: ${r.id} &nbsp; route: ${r.route || ''} &nbsp; ${new Date(r.timestamp).toISOString()}</p>
      <h2>files (${refs.length})</h2><ul>${fileLinks || '<li class=muted>none</li>'}</ul>
      <h2>request_body</h2>${pretty(r.request_body)}
      <h2>response_body</h2>${pretty(r.response_body)}`));
    return;
  }

  // JSON API.
  if (req.method === 'GET' && path === '/api/logs') {
    const limit = Math.min(Number(u.searchParams.get('limit') || 50), 200);
    const offset = Number(u.searchParams.get('offset') || 0);
    const rows = await sql`
      SELECT id, timestamp, method, status_code, latency,
             left(coalesce(request_body,''),200) AS request_preview,
             left(coalesce(response_body,''),200) AS response_preview,
             request_body, response_body
      FROM chat_logs ORDER BY timestamp DESC LIMIT ${limit} OFFSET ${offset}`;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(rows.map(r => ({
      ...r, refs: [...new Set([...extractRefs(r.request_body), ...extractRefs(r.response_body)])],
    }))));
    return;
  }
  if (req.method === 'GET' && path.startsWith('/api/logs/')) {
    const id = decodeURIComponent(path.slice('/api/logs/'.length));
    const rows = await sql`SELECT * FROM chat_logs WHERE id = ${id} LIMIT 1`;
    if (!rows.length) { res.writeHead(404); res.end('{}'); return; }
    const r = rows[0];
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ...r, refs: [...new Set([...extractRefs(r.request_body), ...extractRefs(r.response_body)])] }));
    return;
  }

  res.writeHead(404); res.end('not found');
});

server.listen(3000, () => console.log('Log collector listening on :3000'));
