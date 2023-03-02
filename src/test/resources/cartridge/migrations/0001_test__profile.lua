return {
    up = function()
        local utils = require("migrator.utils")

        local s = box.schema.create_space("test__profile", { if_not_exists = true })

        s:format({
            { name = "profile_id", type = "unsigned", is_nullable = false },
            { name = "bucket_id", type = "unsigned", is_nullable = false },
            { name = "fio", type = "string", is_nullable = false },
            { name = "age", type = "unsigned", is_nullable = false },
            { name = "balance", type = "unsigned", is_nullable = true },
        })

        s:create_index("profile_id", {
            parts = { "profile_id" },
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

        utils.register_sharding_key("test__profile", { "profile_id" })
        
        return true
    end
}
