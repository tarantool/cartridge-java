box.cfg { listen = {
    uri = 3301,
    params = {
        transport = 'ssl',
        ssl_key_file = 'server.key',
        ssl_cert_file = 'server.crt',
        ssl_ca_file = 'ca.crt'
    }
} }

box.schema.user.create('test_user', { password = 'test_password', if_not_exists = true })
box.schema.user.grant('test_user', 'read, write, execute', 'universe', nil, { if_not_exists = true })
