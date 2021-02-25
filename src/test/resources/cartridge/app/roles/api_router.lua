local vshard = require('vshard')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.api_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

local function truncate_space(space_name)
    local storages = cartridge_rpc.get_candidates('app.roles.api_storage')
    cartridge_pool.map_call('box.schema.space[' .. space_name .. ']:truncate', nil, {uri_list = storages})
end

local retries_holder = {
}
local function setup_retrying_function(retries)
    retries_holder.attempts = retries
end

local function retrying_function()
    if (retries_holder.attempts and retries_holder.attempts > 0) then
        retries_holder.attempts = retries_holder.attempts - 1
        return nil, "Unsuccessful attempt"
    else
        return "Success"
    end
end

local function get_composite_data(id)
    local data = vshard.router.callro(vshard.router.bucket_id(id), 'get_composite_data', {id})
    return data
end

local function init(opts)

    rawset(_G, 'truncate_space', truncate_space)

    rawset(_G, 'ddl', { get_schema = get_schema })
    rawset(_G, 'get_composite_data', get_composite_data)
    rawset(_G, 'setup_retrying_function', setup_retrying_function)
    rawset(_G, 'retrying_function', retrying_function)

    return true
end

return {
    role_name = 'app.roles.api_router',
    init = init,
    get_composite_data = get_composite_data,
    dependencies = {
        'cartridge.roles.crud-router',
    }
}
