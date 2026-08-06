// Log collector — receives POST from Kong's http-log plugin, writes to chat_log DB.
// ponytail: ~50 lines, no framework. Runs on node:22-alpine via ConfigMap mount.
import http from 'node:http';
import postgres from 'postgres';

const sql = postgres(process.env.DATABASE_URL);

// Ensure the table exists on startup (idempotent).
await sql`
  CREATE TABLE IF NOT EXISTS chat_logs (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp     timestamptz NOT NULL DEFAULT now(),
    route         text,
    method        text,
    request_body  jsonb,
    response_body jsonb,
    status_code   int,
    latency       int
  )
`;

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
      const payload = JSON.parse(body);
      // Debug: log payload keys to see what Kong sends.
      console.log('Received log payload keys:', Object.keys(payload));
      console.log('Request keys:', payload.request ? Object.keys(payload.request) : 'none');
      console.log('Response keys:', payload.response ? Object.keys(payload.response) : 'none');
      // Kong's http-log plugin sends: { request, response, route, ... }
      // request = { method, uri, headers, body }
      // response = { status, headers, body }
      await sql`
        INSERT INTO chat_logs (route, method, request_body, response_body, status_code, latency)
        VALUES (
          ${payload.route?.name || null},
          ${payload.request?.method || null},
          ${payload.request?.body || null},
          ${payload.response?.body || null},
          ${payload.response?.status || null},
          ${payload.latency?.request || null}
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
