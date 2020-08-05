box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024, -- 128 Mb
    -- log = 'file:/tmp/tarantool.log',
    log_level = 7,
}
-- API user will be able to login with this password
box.schema.user.create('api_user', { password = 'secret' })
-- API user will be able to create spaces, add or remove data, execute functions
box.schema.user.grant('api_user', 'read,write,execute', 'universe')

-- create test space
s = box.schema.space.create('test_space')
s:format({
    {name = 'id', type = 'unsigned'},
    {name = 'book_name', type = 'string'},
    {name = 'author', type = 'string'},
    {name = 'year', type = 'unsigned'}
})
s:create_index('primary', {
    type = 'tree',
    parts = {'id'}
})
s:insert{1, 'Don Quixote', 'Miguel de Cervantes', 1605}
s:insert{2, 'The Great Gatsby', 'F. Scott Fitzgerald', 1925}
s:insert{3, 'War and Peace', 'Leo Tolstoy', 1869}
