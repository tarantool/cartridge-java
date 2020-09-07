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
	{name = 'number_field', type = 'number', is_nullable=true}
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

function user_function_no_param()
    return 5;
end

function user_function_two_param(a, b)
    return a, b, 'Hello, '..a..' '..b;
end