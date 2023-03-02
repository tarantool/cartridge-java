return {
    up = function()
        local utils = require("migrator.utils")

        local s = box.schema.create_space("instances_info", { if_not_exists = true })

        s:format({
            { name = "id", type = "unsigned", is_nullable = false },
            { name = "bucket_id", type = "unsigned", is_nullable = false },
            { name = "uuid", type = "string", is_nullable = false },
            { name = "status", type = "string", is_nullable = false },
            { name = "uri", type = "string", is_nullable = false },
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

        utils.register_sharding_key("instances_info", { "id" })
        
        return true
    end
}
