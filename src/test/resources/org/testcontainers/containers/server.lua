box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024, -- 128 Mb
    -- log = 'file:/tmp/tarantool.log',
    log_level = 6,
}
-- API user will be able to login with this password
box.schema.user.create('api_user', { password = 'secret' })
-- API user will be able to create spaces, add or remove data, execute functions
box.schema.user.grant('api_user', 'read,write,execute', 'universe')

box.schema.user.create('empty_password_user', { password = '' })
box.schema.user.grant('empty_password_user', 'read,write,execute', 'universe')

-- create test space
s = box.schema.space.create('test_space')
s:format({
    {name = 'id', type = 'unsigned'},
    {name = 'unique_key', type = 'string'},
    {name = 'book_name', type = 'string'},
    {name = 'author', type = 'string'},
    {name = 'year', type = 'unsigned',is_nullable=true},
    {name = 'test_delete', type = 'unsigned',is_nullable=true},
    {name = 'test_delete_2', type = 'unsigned',is_nullable=true},
    {name = 'number_field', type = 'number', is_nullable=true},
})
s:create_index('primary', {
    type = 'tree',
    parts = {'id'}
})
s:create_index('inx_author', {
    type = 'tree',
    unique = false,
    parts = {'author'}
})
s:create_index('secondary', {
    type = 'hash',
    unique = true,
    parts = {'unique_key'}
})

s:insert{1, 'a1', 'Don Quixote', 'Miguel de Cervantes', 1605}
s:insert{2, 'a2', 'The Great Gatsby', 'F. Scott Fitzgerald', 1925}
s:insert{3, 'a3', 'War and Peace', 'Leo Tolstoy', 1869}

-- cursor test spaces
c = box.schema.space.create('cursor_test_space')
c:format({
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' },
    { name = 'year', type = 'unsigned' },
});
c:create_index('primary', {
    type = 'tree',
    parts = {'id'}
})

m = box.schema.space.create('cursor_test_space_multi_part_key')
m:format({
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' },
    { name = 'year', type = 'unsigned' },
});

m:create_index('primary', {
    type = 'tree',
    parts = {'id', 'name'}
})

local function tarantool_version()
    local major_minor_patch = _G._TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    return major, minor, patch
end

local major, minor, patch = tarantool_version()
if major >= 2 and minor >= 4 and patch > 1 then
    -- test space for check uuid
    local space_with_uuid = box.schema.space.create(
            'space_with_uuid',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'uuid_field', 'uuid',},
                },
                if_not_exists = true,
            }
    )
    space_with_uuid:create_index('id', { parts = { 'id' }, if_not_exists = true, })
end
if major >= 2 and minor >= 2 and patch > 1 then
    -- test space for check varbinary
    local space_with_varbinary = box.schema.space.create(
            'space_with_varbinary',
            {
                format = {
                    { 'id', 'unsigned' },
                    { 'varbinary_field', 'varbinary',},
                },
                if_not_exists = true,
            }
    )
    space_with_varbinary:create_index('id', { parts = { 'id' }, if_not_exists = true, })
end

--functions

function user_function_no_param()
    return 5;
end

function user_function_two_param(a, b)
    return a, b, 'Hello, '..a..' '..b;
end

function user_function_return_long_value()
    local s = {}
    for i = 1,2800*3 do
        table.insert(s, 1)
    end
    return s
end

function user_function_complex_query(year)
    return s:pairs():filter(function(b) return b.year > year end):totable()
end
