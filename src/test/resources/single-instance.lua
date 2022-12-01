box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024, -- 128 Mb
    log_level = 6,
}

-- Create region space
-- Set if_not_exists = true to ignore that region space already exists
-- You need this to have an ability to restart Tarantool app without deleting .xlog .snap files
s = box.schema.space.create('region', { if_not_exists = true })
s:format({
    { name = 'id', type = 'unsigned' },
    { name = 'name', type = 'string' }
})
s:create_index('id', {
    type = 'tree',
    parts = { 'id' },
    if_not_exists = true
})

-- API user will be able to login with this password
box.schema.user.create('api_user', { password = 'secret' })
-- API user will be able to create spaces, add or remove data, execute functions
box.schema.user.grant('api_user', 'read,write,execute', 'universe')
