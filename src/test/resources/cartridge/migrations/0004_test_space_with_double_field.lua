return {
    up = function()
        local function get_field_type_by_version()
            local tarantoolVersion = box.info.version
            --todo: change this solution for more generic cases like for 10+ versions
            local version = tonumber(string.sub(tarantoolVersion, 1, 3))

            if version >= 2.3 then
                return 'double'
            end

            return 'number'
        end

        local utils = require("migrator.utils")

        local s = box.schema.create_space("test_space_with_double_field", { if_not_exists = true })

        s:format({
            { name = "id", type = "unsigned", is_nullable = false },
            { name = "bucket_id", type = "unsigned", is_nullable = false },
            { name = "double_field", type = get_field_type_by_version(), is_nullable = false },
            { name = "number_field", type = "number", is_nullable = false },
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

        utils.register_sharding_key("test_space_with_double_field", { "id" })
        
        return true
    end
}
