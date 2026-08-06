local typedefs = require "kong.db.schema.typedefs"

return {
  name = "body-capture",
  fields = {
    { consumer = typedefs.no_consumer },
    { protocols = typedefs.protocols },
    { config = {
        type = "record",
        fields = {
          -- Max bytes to capture per request/response body. Guards memory against
          -- pathological payloads. 10MB covers large chat contexts comfortably.
          { max_body_size = { type = "integer", default = 10485760 } },
        },
      } },
  },
}
