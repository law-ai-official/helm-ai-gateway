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
  -- get_raw_body buffers the body; Kong still forwards it upstream unchanged.
  kong.ctx.shared.bc_start = ngx.now()
  local body = kong.request.get_raw_body()
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

  local payload = {
    route     = { name = route and route.name or nil },
    request   = { method = kong.request.get_method(), uri = kong.request.get_path(), body = req_body },
    response  = { status = kong.response.get_status(), body = resp_body },
    latencies = { request = math.floor((ngx.now() - start) * 1000) },
  }

  local json, err = cjson.encode(payload)
  if not json then
    kong.log.err("body-capture: json encode failed: ", err)
    return
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
