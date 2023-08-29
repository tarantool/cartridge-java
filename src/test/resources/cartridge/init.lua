#!/usr/bin/env tarantool

require('strict').on()

if package.setsearchroot ~= nil then
    package.setsearchroot()
end

local cartridge = require('cartridge')
local ok, err = cartridge.cfg({
    workdir = 'tmp/db',
    roles = {
        'cartridge.roles.crud-storage',
        'cartridge.roles.crud-router',
        'app.roles.api_router',
        'app.roles.api_storage',
        'app.roles.custom',
    },
    cluster_cookie = 'testapp-cluster-cookie',
}, {
    readahead = 10 * 1024 * 1024, -- 10 MB
    net_msg_max = 11140,
})

assert(ok, tostring(err))
