local function init_space()
    local profile = box.schema.space.create(
            'test__profile',
            {
                format = {
                    { 'profile_id', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                    { 'fio', 'string' },
                    { 'age', 'unsigned' },
                    { 'balance', 'unsigned', is_nullable = true }
                },
                if_not_exists = true,
            }
    )

    profile:create_index('profile_id', { parts = { 'profile_id' }, if_not_exists = true, })
    profile:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    local test_space = box.schema.space.create(
            'test_space',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                    { 'field1', 'string' },
                    { 'field2', 'unsigned' },
                },
                if_not_exists = true,
            }
    )

    test_space:create_index('id', { parts = { 'id' }, if_not_exists = true, })
    test_space:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    local second_test_space = box.schema.space.create(
            'second_test_space',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                    { 'field1', 'string' },
                    { 'field2', 'unsigned' },
                },
                if_not_exists = true,
            }
    )

    second_test_space:create_index('id', { parts = { 'id' }, if_not_exists = true, })
    second_test_space:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    local test_space_to_join = box.schema.space.create(
            'test_space_to_join',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                    { 'field3', 'boolean' },
                    { 'field4', 'number' },
                },
                if_not_exists = true,
            }
    )

    test_space_to_join:create_index('id', { parts = { 'id' }, if_not_exists = true, })
    test_space_to_join:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    -- test space for check double (cdata) field in it
    local test_space_with_double_field = box.schema.space.create(
            'test_space_with_double_field',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                    { 'double_field', get_field_type_by_version() },
                    { 'number_field', 'number' },
                },
                if_not_exists = true,
            }
    )

    test_space_with_double_field:create_index('id', { parts = { 'id' }, if_not_exists = true, })
    test_space_with_double_field:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    -- cursor test spaces
    local cursor_test_space = box.schema.space.create('cursor_test_space', { if_not_exists = true })
    cursor_test_space:format({
        { name = 'id', type = 'unsigned' },
        { name = 'name', type = 'string' },
        { name = 'year', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
    });
    cursor_test_space:create_index('primary', {
        type = 'tree',
        parts = { 'id' },
        if_not_exists = true,
    })
    cursor_test_space:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
        type = 'TREE'
    })

    local cursor_test_space_multi_part_key = box.schema.space.create(
            'cursor_test_space_multi_part_key', { if_not_exists = true })
    cursor_test_space_multi_part_key:format({
        { name = 'id', type = 'unsigned' },
        { name = 'name', type = 'string' },
        { name = 'year', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
    });
    cursor_test_space_multi_part_key:create_index('primary', {
        type = 'tree',
        parts = { 'id', 'name' },
        if_not_exists = true,
    })
    cursor_test_space_multi_part_key:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
        type = 'TREE'
    })

    local test_space_with_byte_array = box.schema.space.create(
             'test_space_with_byte_array', { if_not_exists = true })
    test_space_with_byte_array:format({
        { name = 'id', type = 'unsigned' },
        { name = 'bucket_id', type = 'unsigned' },
        { name = 'value', type = 'varbinary' },
    });
    test_space_with_byte_array:create_index('primary', {
        type = 'tree',
        parts = { 'id' },
        if_not_exists = true,
    })
    test_space_with_byte_array:create_index('bucket_id', {
        type = 'tree',
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })

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

function get_field_type_by_version()
    local tarantoolVersion = box.info.version
    --todo: change this solution for more generic cases like for 10+ versions
    local version = tonumber(string.sub(tarantoolVersion, 1, 3))

    if version >= 2.3 then
        return 'double'
    end

    return 'number'
end

local function init(opts)
    if opts.is_master then
        init_space()
    end

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
