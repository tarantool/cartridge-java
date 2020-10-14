local crud = require('crud')

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

    profile:create_index('primary', { parts = { 'profile_id' }, if_not_exists = true, })
    profile:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

    -- create cursor test space
    local cursor_test_space = box.schema.space.create('cursor_test_space',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'name', 'string' },
                    { 'year', 'unsigned' },
                    { 'bucket_id', 'unsigned' },
                },
                if_not_exists = true,
            })

    cursor_test_space:create_index('primary', { type = 'tree', parts = { 'id' }, if_not_exists = true, })
    cursor_test_space:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })

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

    test_space:create_index('primary', { parts = { 'id' }, if_not_exists = true, })
    test_space:create_index('bucket_id', { parts = { 'bucket_id' }, unique = false, if_not_exists = true, })
end

local function init(opts)
    -- initialize crud
    crud.init()

    if opts.is_master then
        init_space()
    end

    return true
end

return {
    role_name = 'app.roles.api_storage',
    init = init,
    dependencies = {
        'cartridge.roles.vshard-storage'
    }
}
