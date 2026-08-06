// Log collector - receives POSTs from Kong's body-capture plugin (full request +
// response bodies) and writes them to the chat_log DB.
//
// Bodies are stored as TEXT (not jsonb): chat completion responses are often SSE
// streams, which are not valid JSON and would break a jsonb insert. Text stores
// anything reliably; cast to jsonb when querying request bodies.
// ponytail: ~60 lines, no framework. Runs on node:22-alpine via ConfigMap mount.
import http from 'node:http';
import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL);

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
  // column_name is a real column name from our own query above; safe to interpolate.
  await sql.unsafe(`ALTER TABLE chat_logs ALTER COLUMN ${column_name} TYPE text USING ${column_name}::text`);
}

const server = http.createServer(async (req, res) => {
  // Health check endpoint for readiness probe.
  if (req.method === 'GET' && (req.url === '/health' || req.url === '/')) {
    res.writeHead(200);
    res.end('OK');
    return;
  }

  if (req.method !== 'POST' || req.url !== '/logs') {
    res.writeHead(404);
    res.end('Not found');
    return;
  }

  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    try {
      const p = JSON.parse(body);
      // Payload shape sent by the body-capture plugin:
      //   { route:{name}, request:{method,uri,body}, response:{status,body}, latencies:{request} }
      await sql`
        INSERT INTO chat_logs (route, method, request_body, response_body, status_code, latency)
        VALUES (
          ${p.route?.name || null},
          ${p.request?.method || null},
          ${p.request?.body || null},
          ${p.response?.body || null},
          ${p.response?.status || null},
          ${p.latencies?.request ?? p.latency?.request ?? null}
        )
      `;
      res.writeHead(200);
      res.end('OK');
    } catch (err) {
      console.error('Failed to log:', err.message);
      res.writeHead(500);
      res.end('Error');
    }
  });
});

server.listen(3000, () => {
  console.log('Log collector listening on :3000');
});
