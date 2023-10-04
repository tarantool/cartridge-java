local fiber = require('fiber')
local log = require('log')
local metadata_utils = require('utils.metadata')

local function reset_request_counters()
    box.space.request_counters:replace({ 1, 0 })
end

local function update_request_counters(with_session_id)
    with_session_id = with_session_id or false
    -- need using number instead field name as string in update function for compatibility with tarantool 1.10
    if with_session_id then
        box.space.request_counters:update(box.session.id(), { { '+', 2, 1 } })
    else
        box.space.request_counters:update(1, { { '+', 2, 1 } })
    end
end

local function get_request_count()
    return box.space.request_counters:get(1)[2]
end

local function simple_long_running_function(seconds_to_sleep, with_session_id)
    update_request_counters(with_session_id)
    fiber.sleep(seconds_to_sleep)
    return true
end

local function long_running_function(values, with_session_id)
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

    update_request_counters(with_session_id)
    log.info('Executing long-running function ' ..
        tostring(box.space.request_counters:get(1)[2]) ..
        "(name: " .. disabled_router_name ..
        "; sleep: " .. seconds_to_sleep .. ")")
    if metadata_utils.get_router_name() == disabled_router_name then
        return nil, "Disabled by client; router_name = " .. disabled_router_name
    end
    if seconds_to_sleep then
        fiber.sleep(seconds_to_sleep)
    end
    return true
end

local function reset_request_counters_on_connect()
    box.space.request_counters:replace({ box.session.id(), 0 })
end

local function init_counter_space()
    local request_counters = box.schema.space.create('request_counters', {
        format = { { 'id', 'unsigned' }, { 'count', 'unsigned' } },
        if_not_exists = true
    })
    request_counters:create_index('id', { parts = { 'id' }, if_not_exists = true })
    request_counters:create_index('count', { parts = { 'count' }, if_not_exists = true, unique = false })


    box.session.on_connect(reset_request_counters_on_connect)
end

return {
    reset_request_counters = reset_request_counters,
    simple_long_running_function = simple_long_running_function,
    long_running_function = long_running_function,
    get_request_count = get_request_count,
    init_counter_space = init_counter_space,
}
