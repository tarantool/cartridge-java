box.cfg { listen = {
    uri = 3301,
    params = {
        transport = 'ssl',
        ssl_key_file = 'key.pem',
        ssl_cert_file = 'certificate.crt'
    }
} }

box.schema.user.create('test_user', { password = 'test_password', if_not_exists = true })
box.schema.user.grant('test_user', 'read, write, execute', 'universe', nil, { if_not_exists = true })
