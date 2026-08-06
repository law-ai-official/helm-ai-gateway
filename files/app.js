// Log collector - receives POSTs from Kong's body-capture plugin (full request +
// response bodies), offloads large inline payloads (base64 images/files) to
// MinIO, and writes the rest to the chat_log DB.
//
// Flow per request:
//   1. Parse request_body / response_body as JSON (skip if not JSON, e.g. SSE).
//   2. Recursively walk for `data:` URIs above THRESHOLD; upload each to MinIO
//      and replace the string with `minio://<bucket>/<key>`.
//   3. Store the (now small) bodies in chat_logs.
//
// Bodies are TEXT (not jsonb): SSE streams and replaced references are not
// JSON objects. Cast to jsonb when querying.
// ponytail: ~110 lines, no framework. Runs on node:22-alpine via ConfigMap mount.
import http from 'node:http';
import crypto from 'node:crypto';
import postgres from 'postgres';
import Minio from 'minio';

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

// Migrate legacy jsonb columns -> text (idempotent; safe to re-run on every boot).
const jsonbCols = await sql`
  SELECT column_name FROM information_schema.columns
  WHERE table_name = 'chat_logs' AND data_type = 'jsonb'
`;
for (const { column_name } of jsonbCols) {
  await sql.unsafe(`ALTER TABLE chat_logs ALTER COLUMN ${column_name} TYPE text USING ${column_name}::text`);
}

// Only offload data URIs larger than this; tiny inline assets stay in the row.
const THRESHOLD = 4096;

const MIME_EXT = {
  'image/png': 'png', 'image/jpeg': 'jpg', 'image/gif': 'gif', 'image/webp': 'webp',
  'application/pdf': 'pdf', 'audio/mpeg': 'mp3', 'audio/wav': 'wav',
  'text/plain': 'txt', 'application/octet-stream': 'bin',
};

let assetSeq = 0;

// Upload a single `data:<mime>;base64,<...>` URI to MinIO; return a minio:// ref.
async function uploadDataUri(dataUri, group) {
  const m = dataUri.match(/^data:([^;]+);base64,(.*)$/s);
  if (!m) return dataUri;                       // not a base64 data URI; leave as-is
  const contentType = m[1];
  const buf = Buffer.from(m[2], 'base64');
  const ext = MIME_EXT[contentType] || 'bin';
  const key = `${group}/${Date.now()}-${assetSeq++}.${ext}`;
  try {
    await minio.putObject(BUCKET, key, buf, buf.length, { 'Content-Type': contentType });
    return `minio://${BUCKET}/${key}`;
  } catch (e) {
    console.error('minio upload failed, keeping inline:', e.message);
    return dataUri;                             // fall back so logging still succeeds
  }
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

// If `body` is a JSON string, offload its data URIs and return the rewritten string.
async function processBody(body, group) {
  if (typeof body !== 'string' || body.length === 0) return body;
  const trimmed = body.trimStart();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return body;  // not JSON (SSE/multipart)
  try {
    const parsed = JSON.parse(body);
    await offloadDataUris(parsed, group);
    return JSON.stringify(parsed);
  } catch {
    return body;  // not valid JSON; store as-is
  }
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
      const reqStored = await processBody(p.request?.body, group);
      const respStored = await processBody(p.response?.body, group);
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
