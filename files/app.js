// Log collector - receives POSTs from Kong's body-capture plugin (full request +
// response bodies), offloads large inline payloads to MinIO, writes the rest to DB.
//
// Per request:
//   - If body_b64 flag is set, base64-decode first (binary bodies, e.g. multipart).
//   - If content-type is multipart/form-data: parse with busboy, upload every file
//     part to MinIO, store a JSON description ({_multipart, fields, files[]}) with
//     minio:// refs. Text fields are kept inline.
//   - Else if body is JSON: recursively walk for `data:` URIs > THRESHOLD, upload
//     each to MinIO, replace with `minio://<bucket>/<key>`.
//   - Else (SSE, plain text): store as-is.
//
// Bodies are TEXT (not jsonb): SSE and multipart descriptions aren't JSON objects.
// ponytail: ~150 lines. Runs on node:22-alpine via ConfigMap mount.
import http from 'node:http';
import crypto from 'node:crypto';
import postgres from 'postgres';
import { createRequire } from 'module';
// `minio` and `busboy` ship as CommonJS with no ESM default export; load via require.
const require = createRequire(import.meta.url);
const Minio = require('minio');
const Busboy = require('busboy');

const sql = postgres(process.env.DATABASE_URL);

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

// Only offload data URIs larger than this (tiny inline assets stay in the row).
const THRESHOLD = 4096;
const MIME_EXT = {
  'image/png': 'png', 'image/jpeg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp',
  'application/pdf': 'pdf', 'audio/mpeg': 'mp3', 'audio/wav': 'wav',
  'text/plain': 'txt', 'application/octet-stream': 'bin',
};
let assetSeq = 0;

// Upload a Buffer to MinIO; return a minio:// ref.
async function uploadBuffer(buf, contentType, group, filename) {
  const ext = MIME_EXT[contentType] || (filename && filename.split('.').pop()) || 'bin';
  const safe = (filename || `asset-${assetSeq++}`).replace(/[^a-zA-Z0-9._-]/g, '_');
  const key = `${group}/${Date.now()}-${assetSeq++}-${safe}`;
  await minio.putObject(BUCKET, key, buf, buf.length, { 'Content-Type': contentType || 'application/octet-stream' });
  return `minio://${BUCKET}/${key}`;
}

// Upload a single `data:<mime>;base64,<...>` URI to MinIO; return a minio:// ref.
async function uploadDataUri(dataUri, group) {
  const m = dataUri.match(/^data:([^;]+);base64,(.*)$/s);
  if (!m) return dataUri;
  const buf = Buffer.from(m[2], 'base64');
  try { return await uploadBuffer(buf, m[1], group, `inline.${MIME_EXT[m[1]] || 'bin'}`); }
  catch (e) { console.error('minio upload failed, keeping inline:', e.message); return dataUri; }
}

// Recursively replace large data: URIs with minio:// refs. Mutates `obj` in place.
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

// Parse a multipart body: upload each file part to MinIO, keep text fields inline.
// Returns a JSON string describing the parts.
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

// Process one body (string or base64 string) -> small string for DB storage.
async function processBody(body, bodyB64, contentType, group) {
  if (!body) return body;
  let raw;
  if (bodyB64) {
    raw = Buffer.from(body, 'base64');                // binary-safe decode
  } else {
    raw = body;                                         // already a text string
  }
  // Multipart file upload: extract files to MinIO.
  if (contentType && contentType.startsWith('multipart/form-data') && Buffer.isBuffer(raw)) {
    return await processMultipart(raw, contentType, group);
  }
  // JSON: offload large data: URIs.
  if (typeof raw === 'string') {
    const t = raw.trimStart();
    if (t.startsWith('{') || t.startsWith('[')) {
      try { const p = JSON.parse(raw); await offloadDataUris(p, group); return JSON.stringify(p); }
      catch { return raw; }
    }
    return raw;
  }
  // Binary but not multipart (rare): store as base64.
  return raw.toString('base64');
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
    res.writeHead(200); res.end('OK'); return;
  }
  if (req.method !== 'POST' || req.url !== '/logs') {
    res.writeHead(404); res.end('Not found'); return;
  }

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
          ${p.route?.name || null},
          ${p.request?.method || null},
          ${reqStored || null},
          ${respStored || null},
          ${p.response?.status || null},
          ${p.latencies?.request ?? p.latency?.request ?? null}
        )
      `;
      res.writeHead(200); res.end('OK');
    } catch (err) {
      console.error('Failed to log:', err.message);
      res.writeHead(500); res.end('Error');
    }
  });
});

server.listen(3000, () => console.log('Log collector listening on :3000'));
