-- Kong pre-function plugin: capture request/response bodies and send to log-collector
-- ponytail: ~80 lines, runs in Kong's Lua environment

local http = require "resty.http"

return {
  -- Access phase: read and cache request body
  access = function(self, conf)
    local body, err = kong.request.get_raw_body()
    if body then
      kong.ctx.shared.request_body = body
    end
  end,

  -- Body filter phase: capture response body chunks
  body_filter = function(self, conf)
    local chunk = ngx.arg[1]
    local eof = ngx.arg[2]

    if chunk and chunk ~= "" then
      local existing = kong.ctx.shared.response_body or ""
      kong.ctx.shared.response_body = existing .. chunk
    end
  end,

  -- Log phase: send captured bodies to log-collector
  log = function(self, conf)
    local request_body = kong.ctx.shared.request_body or ""
    local response_body = kong.ctx.shared.response_body or ""

    -- Only log if we have bodies (avoid logging health checks, etc.)
    if request_body == "" and response_body == "" then
      return
    end

    local log_data = {
      route = kong.router.get_route(),
      service = kong.router.get_service(),
      request = {
        method = kong.request.get_method(),
        uri = kong.request.get_path(),
        headers = kong.request.get_headers(),
        body = request_body
      },
      response = {
        status = kong.response.get_status(),
        headers = kong.response.get_headers(),
        body = response_body
      },
      latencies = {
        request = kong.request.get_header("x-kong-request-start") and
                  (ngx.now() * 1000 - tonumber(kong.request.get_header("x-kong-request-start"))) or nil
      },
      started_at = ngx.now() * 1000
    }

    -- Send to log-collector asynchronously
    ngx.timer.at(0, function(premature)
      if premature then return end

      local httpc = http.new()
      httpc:set_timeout(5000)

      local ok, err = httpc:connect("log-collector", 3000)
      if not ok then
        kong.log.err("Failed to connect to log-collector: ", err)
        return
      end

      local res, err = httpc:request({
        method = "POST",
        path = "/logs",
        headers = {
          ["Content-Type"] = "application/json"
        },
        body = cjson.encode(log_data)
      })

      if not res then
        kong.log.err("Failed to send log: ", err)
      end

      httpc:close()
    end)
  end
}
