return {
    up = function()
        local utils = require("migrator.utils")

        local s = box.schema.create_space("cursor_test_space_multi_part_key", { if_not_exists = true })

        s:format({
            { name = "id", type = "unsigned", is_nullable = false },
            { name = "name", type = "string", is_nullable = false },
            { name = "year", type = "unsigned", is_nullable = false },
            { name = "bucket_id", type = "unsigned", is_nullable = false },
        })

        s:create_index("primary", {
            parts = { "id", "name" },
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

        utils.register_sharding_key("cursor_test_space_multi_part_key", { "id" })
        
        return true
    end
}
