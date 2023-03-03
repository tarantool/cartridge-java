local function tarantool_version()
    local major_minor_patch = _G._TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    return major, minor, patch
end

local function get_composite_data(id)
    local composite = { id = id }
    local data1 = box.space.test_space:get(id)
    composite.field1 = data1.field1
    composite.field2 = data1.field2
    local data2 = box.space.test_space_to_join:get(id)
    composite.field3 = data2.field3
    composite.field4 = data2.field4
    return composite
end

local function init(opts)
    rawset(_G, 'get_composite_data', get_composite_data)

    return true
end

return {
    role_name = 'app.roles.api_storage',
    init = init,
    get_composite_data = get_composite_data,
    get_schema = require('ddl').get_schema,
    dependencies = {
        'cartridge.roles.crud-storage'
    }
}
