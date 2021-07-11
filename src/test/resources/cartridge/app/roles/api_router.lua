local vshard = require('vshard')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')
local fiber = require('fiber')
local log = require('log')

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.api_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

local function truncate_space(space_name)
    crud.truncate(space_name)
end

local retries_holder = {
}
local function setup_retrying_function(retries)
    retries_holder.attempts = retries
end

local function retrying_function()
    if (retries_holder.attempts and retries_holder.attempts > 0) then
        log.info("retrying_function attempts = " .. retries_holder.attempts)
        retries_holder.attempts = retries_holder.attempts - 1
        return nil, "Unsuccessful attempt " .. retries_holder.attempts
    else
        return "Success"
    end
end

local function get_composite_data(id)
    local data = vshard.router.callro(vshard.router.bucket_id(id), 'get_composite_data', {id})
    return data
end

local function get_rows_as_multi_result(space_name)
    local data = crud.select(space_name).rows
    return unpack(data)
end

local function get_array_as_multi_result(numbers)
    return unpack(numbers)
end

local function get_array_as_single_result(numbers)
    return numbers
end

local function returning_error(message)
    return nil, message
end

local function raising_error(message)
    error("Test error: raising_error() called")
end

local function reset_request_counters()
    box.space.request_counters:replace({1, 0})
end

local function get_router_name()
    return string.sub(box.cfg.custom_proc_title, 9)
end

local function long_running_function(values)
    local seconds_to_sleep = 0
    local disabled_router_name = ""
    if values ~= nil then
        if type(values) == "table" then
            values = values or {}
            seconds_to_sleep = values[1]
            disabled_router_name = values[2]
        else
            seconds_to_sleep = values
        end
    end

    box.space.request_counters:update(1, {{'+', 'count', 1}})
    log.info('Executing long-running function ' ..
            tostring(box.space.request_counters:get(1)[2]) ..
            "(name: " .. disabled_router_name ..
            "; sleep: " .. seconds_to_sleep .. ")")
    if get_router_name() == disabled_router_name then
        return nil, "Disabled by client; router_name = " ..  disabled_router_name
    end
    if seconds_to_sleep then fiber.sleep(seconds_to_sleep) end
    return true
end

local function get_request_count()
    return box.space.request_counters:get(1)[2]
end

-- it's like vshard error throwing
local function box_error_unpack_no_connection()
    return nil, box.error.new(box.error.NO_CONNECTION):unpack()
end

local function box_error_unpack_timeout()
    return nil, box.error.new(box.error.TIMEOUT):unpack()
end

local function box_error_non_network_error()
    return nil, box.error.new(box.error.WAL_IO):unpack()
end

local function crud_error_timeout()
    return crud.get("test_space", ('x'):rep(2^27))
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
    rawset(_G, 'get_rows_as_multi_result', get_rows_as_multi_result)
    rawset(_G, 'get_array_as_multi_result', get_array_as_multi_result)
    rawset(_G, 'get_array_as_single_result', get_array_as_single_result)
    rawset(_G, 'returning_error', returning_error)
    rawset(_G, 'setup_retrying_function', setup_retrying_function)
    rawset(_G, 'retrying_function', retrying_function)
    rawset(_G, 'raising_error', raising_error)

    rawset(_G, 'reset_request_counters', reset_request_counters)
    rawset(_G, 'get_router_name', get_router_name)
    rawset(_G, 'long_running_function', long_running_function)
    rawset(_G, 'get_request_count', get_request_count)
    rawset(_G, 'box_error_unpack_no_connection', box_error_unpack_no_connection)
    rawset(_G, 'box_error_unpack_timeout', box_error_unpack_timeout)
    rawset(_G, 'box_error_non_network_error', box_error_non_network_error)
    rawset(_G, 'crud_error_timeout', crud_error_timeout)

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
