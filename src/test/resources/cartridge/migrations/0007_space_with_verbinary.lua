local tarantool_version = require("utils.migration").tarantool_version

local major, minor, patch = tarantool_version()
if major >= 2 and minor >= 4 and patch > 1 then
    return {
        up = function()
            local utils = require("migrator.utils")

            local s = box.schema.create_space("space_with_varbinary", { if_not_exists = true })

            s:format({
                { name = "id", type = "unsigned", is_nullable = false },
                { name = "varbinary_field", type = "varbinary", is_nullable = false },
                { name = "bucket_id", type = "unsigned", is_nullable = false },
            })

            s:create_index("id", {
                parts = { "id" },
                unique = true,
                if_not_exists = true,
                type = "TREE"
            })

            s:create_index("bucket_id", {
                parts = { "bucket_id" },
                unique = false,
                if_not_exists = true,
                type = "TREE"
            })

            utils.register_sharding_key("space_with_varbinary", { "id" })

            return true
        end
    }
end
