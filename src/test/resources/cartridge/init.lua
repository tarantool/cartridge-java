#!/usr/bin/env tarantool

require('strict').on()

if package.setsearchroot ~= nil then
    package.setsearchroot()
end

require('migrator')

local cartridge = require('cartridge')
local ok, err = cartridge.cfg({
    workdir = 'tmp/db',
    roles = {
        'cartridge.roles.crud-storage',
        'cartridge.roles.crud-router',
        'app.roles.api_router',
        'app.roles.api_storage',
        'app.roles.custom',
        'migrator',
    },
    cluster_cookie = 'testapp-cluster-cookie',
})

assert(ok, tostring(err))
