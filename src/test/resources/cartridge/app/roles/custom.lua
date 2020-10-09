local cartridge = require('cartridge')

local function get_routers()
    local function table_contains(table, element)
        for _, value in pairs(table) do
            if value == element then
                return true
            end
        end
        return false
    end

    local servers, err = cartridge.admin_get_servers()
    local routers = {}

    for _, server in pairs(servers) do
        if server.replicaset ~= nil then
            if table_contains(server.replicaset.roles, 'app.roles.custom') then
                routers[server.uuid] = {
                    status = server.status,
                    uuid = server.uuid,
                    uri = server.uri,
                    priority = server.priority
                }
            end
        end
    end

    return routers
end

local function init_httpd()
    local httpd = cartridge.service_get('httpd')

    httpd:route({ method = 'GET', path = '/routers' }, function(req)
        local json = require('json')
        local result = get_routers();

        return { body = json.encode(result) }
    end)
end

local function init(opts)
    -- luacheck: no unused args
    if opts.is_master then
        box.schema.user.grant('guest', 'read,write', 'universe', nil, { if_not_exists = true })
    end

    init_httpd()

    rawset(_G, 'get_routers', get_routers)

    return true
end

local function stop()
end

local function validate_config(conf_new, conf_old)
    -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts)
    -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    return true
end

return {
    role_name = 'app.roles.custom',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = { 'cartridge.roles.vshard-router' },
}
