local vshard = require('vshard')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')
local fiber = require('fiber')

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
        local conn = cartridge_pool.connect(instance_uri)
        return conn:call('ddl.get_schema', {})
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

local function reset_request_counters()
    box.space.request_counters:replace({1, 0})
end

local function long_running_function(seconds_to_sleep)
    require('log').info('Executing long-running function')
    box.space.request_counters:update(1, {{'+', 'count', 1}})
    if seconds_to_sleep then fiber.sleep(seconds_to_sleep) end
    return true
end

local function get_request_count()
    return box.space.request_counters:get(1)[2]
end

local function init(opts)

    box.schema.space.create('request_counters', {
        format = {{'id', 'unsigned'}, {'count', 'unsigned'}},
        if_not_exists = true
    })
    box.space.request_counters:create_index('primary', {parts = {'id'}, if_not_exists = true})

    rawset(_G, 'truncate_space', truncate_space)

    rawset(_G, 'ddl', { get_schema = get_schema })
    rawset(_G, 'get_composite_data', get_composite_data)
    rawset(_G, 'setup_retrying_function', setup_retrying_function)
    rawset(_G, 'retrying_function', retrying_function)

    rawset(_G, 'reset_request_counters', reset_request_counters)
    rawset(_G, 'long_running_function', long_running_function)
    rawset(_G, 'get_request_count', get_request_count)

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
