-- body-capture plugin: captures full request and response bodies and forwards
-- them to log-collector, which stores them in the chat_log.chat_logs table.
--
-- Replaces Kong's bundled http-log for this route: http-log only emits
-- request/response metadata (headers, status, size) and NEVER the body, so
-- conversation content was never reaching the database. This plugin reads the
-- body in `access`, accumulates response chunks in `body_filter`, and ships
-- the pair to log-collector in `log` via an async timer (never blocks the reply).
--
-- No external deps: uses only the Kong PDK + ngx.socket.tcp (bundled in Kong).

local cjson = require "cjson.safe"

local BodyCapture = {
  PRIORITY = 1001,   -- run early in access, before any body-transforming plugin
  VERSION = "1.0.0",
}

function BodyCapture:access(conf)
  -- Stamp start time for latency; read+cache the request body.
  -- Pass max_body_size so Kong reads bodies that spilled to a temp file (bodies
  -- larger than nginx client_body_buffer_size, 8KB by default). Without this arg
  -- get_raw_body() returns nil for any body that doesn't fit the in-memory buffer.
  -- Bodies larger than max_body_size are rejected (nil) without reading them.
  kong.ctx.shared.bc_start = ngx.now()
  local body = kong.request.get_raw_body(conf.max_body_size)
  if body and body ~= "" then
    local max = conf.max_body_size
    kong.ctx.shared.req_body = (#body > max) and string.sub(body, 1, max) or body
  end
end

function BodyCapture:body_filter(conf)
  -- Accumulate response chunks as they stream through. Works for both buffered
  -- and SSE/streaming responses (we just concatenate every chunk we see).
  local chunk = ngx.arg[1]
  if not chunk or chunk == "" then return end
  local cur = kong.ctx.shared.resp_body or ""
  if #cur >= conf.max_body_size then return end   -- cap reached, drop the rest
  local combined = cur .. chunk
  if #combined > conf.max_body_size then
    combined = string.sub(combined, 1, conf.max_body_size)
  end
  kong.ctx.shared.resp_body = combined
end

function BodyCapture:log(conf)
  local req_body = kong.ctx.shared.req_body
  local resp_body = kong.ctx.shared.resp_body
  -- Skip requests with nothing to log (e.g. GET /v1/models with empty request).
  if not req_body and not resp_body then return end

  local route = kong.router.get_route()
  local start = kong.ctx.shared.bc_start or ngx.now()
  local req_ct = kong.request.get_header("Content-Type")
  local resp_ct = kong.response.get_header("Content-Type")

  -- Multipart bodies are binary; base64 them up front so they survive JSON
  -- transit intact. We can't rely on cjson failing: it may silently mangle
  -- non-UTF-8 bytes into a string with NULs, which then breaks the DB insert
  -- and can't be re-parsed on the collector side.
  local req_b64  = req_body  and req_ct  and req_ct:find("multipart/form-data")  ~= nil
  local resp_b64 = resp_body and resp_ct and resp_ct:find("multipart/form-data") ~= nil
  local req_body_enc  = req_b64  and ngx.encode_base64(req_body)  or req_body
  local resp_body_enc = resp_b64 and ngx.encode_base64(resp_body) or resp_body

  local payload = {
    route     = { name = route and route.name or nil },
    request   = { method = kong.request.get_method(), uri = kong.request.get_path(),
                  content_type = req_ct, body = req_body_enc, body_b64 = req_b64 },
    response  = { status = kong.response.get_status(),
                  content_type = resp_ct, body = resp_body_enc, body_b64 = resp_b64 },
    latencies = { request = math.floor((ngx.now() - start) * 1000) },
  }

  local json, err = cjson.encode(payload)
  if not json then
    -- Some other binary body cjson can't encode; base64 anything not already.
    if req_body_enc  and not req_b64  then payload.request.body  = ngx.encode_base64(req_body_enc);  payload.request.body_b64  = true end
    if resp_body_enc and not resp_b64 then payload.response.body = ngx.encode_base64(resp_body_enc); payload.response.body_b64 = true end
    json, err = cjson.encode(payload)
    if not json then
      kong.log.err("body-capture: json encode failed (even after base64): ", err)
      return
    end
  end

  -- Fire-and-forget: deliver in a timer so the client response is never delayed.
  ngx.timer.at(0, function(premature)
    if premature then return end
    local sock = ngx.socket.tcp()
    sock:settimeout(5000)
    local ok, err = sock:connect("log-collector", 3000)
    if not ok then
      kong.log.err("body-capture: connect to log-collector failed: ", err)
      return
    end
    local req = "POST /logs HTTP/1.1\r\nHost: log-collector\r\n"
             .. "Content-Type: application/json\r\nContent-Length: " .. #json
             .. "\r\nConnection: close\r\n\r\n" .. json
    local _, serr = sock:send(req)
    if serr then kong.log.err("body-capture: send failed: ", serr) end
    sock:settimeout(1000)
    sock:receive("*a")   -- drain so the connection closes cleanly
    sock:close()
  end)
end

return BodyCapture
