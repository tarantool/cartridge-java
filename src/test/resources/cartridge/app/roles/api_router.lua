local vshard = require('vshard')
local cartridge_rpc = require('cartridge.rpc')
local crud = require('crud')
local uuid = require('uuid')
local log = require('log')

local metadata_utils = require('utils.metadata')
local crud_utils = require('utils.crud')
local counter = require('modules.counter')

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.api_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

local function truncate_space(space_name)
    crud.truncate(space_name)
end

local crud_methods_to_patch = {
    'select',
    'delete',
    'insert',
    'insert_many',
    'replace',
    'replace_many',
    'update',
    'upsert',
}
local function patch_crud_methods_for_tests()
    for _, name in ipairs(crud_methods_to_patch) do
        local real_method = crud[name]
        crud[name] = function(...)
            local args = { ... }
            rawset(_G, ('crud_%s_opts'):format(name), args[#args])
            return real_method(...)
        end
    end
end

local function get_routers_status()
    local res = crud.select('instances_info')

    local result = {}
    for _, v in pairs(res.rows) do
        result[v[3]] = { status = v[4], uri = v[5] }
    end
    return result
end

local function init_router_status()
    crud.insert('instances_info', { 1, 1, uuid.str(), 'available', 'localhost:3301' })
    crud.insert('instances_info', { 2, 1, uuid.str(), 'available', 'localhost:3302' })
    crud.insert('instances_info', { 3, 1, uuid.str(), 'available', 'localhost:3303' })
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
    local data = vshard.router.callro(vshard.router.bucket_id(id), 'get_composite_data', { id })
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

local function custom_crud_select(space_name)
    return crud.select(space_name)
end

local function raising_error()
    error("Test error: raising_error() called")
end

-- it's like vshard error throwing
local function box_error_unpack_no_connection()
    return nil, box.error.new(box.error.NO_CONNECTION):unpack()
end

local function box_error_unpack_timeout()
    return nil, box.error.new(box.error.TIMEOUT):unpack()
end

local function box_error_timeout()
    return box.error(box.error.TIMEOUT)
end

local function box_error_non_network_error()
    return nil, box.error.new(box.error.WAL_IO):unpack()
end

function delete_with_error_if_not_found(space_name, key, opts)
    local result, err = crud.delete(space_name, key, opts)
    if err then
        return nil, err
    end

    if #result.rows == 0 then
        return nil, "Records not found"
    end

    return result
end

local function test_no_such_procedure()
    return 'test_no_such_procedure'
end

local function crud_error_timeout()
    return nil, { class_name = 'SelectError',
                  err = 'Failed to get next object: GetTupleError: Failed to get tuples from storages: UpdateTuplesError: Failed to select tuples from storages: Call: Failed for 07d14fec-f32b-4b90-aa72-e6755273ad56: Function returned an error: {\"code\":78,\"base_type\":\"ClientError\",\"type\":\"ClientError\",\"message\":\"Timeout exceeded\",\"trace\":[{\"file\":\"builtin\\/box\\/net_box.lua\",\"line\":419}]}',
                  str = 'SelectError: Failed to get next object: GetTupleError: Failed to get tuples from storages: UpdateTuplesError: Failed to select tuples from storages: Call: Failed for 07d14fec-f32b-4b90-aa72-e6755273ad56: Function returned an error: {\"code\":78,\"base_type\":\"ClientError\",\"type\":\"ClientError\",\"message\":\"Timeout exceeded\",\"trace\":[{\"file\":\"builtin\\/box\\/net_box.lua\",\"line\":419}]}'
    }
end

function returning_number()
    return 2
end

local function create_restricted_user()
    box.schema.func.create("returning_number", { if_not_exists = true, setuid = true })

    box.schema.user.create('restricted_user', { if_not_exists = true, password = 'restricted_secret' })
    box.schema.user.grant("restricted_user", "execute", "function", "returning_number", { if_not_exists = true })
end

local function select_router_space()
    return box.space.router_space:select()
end

local function init_router_spaces()
    local router_space = box.schema.space.create('router_space', {
        format = { { 'id', 'unsigned' } },
        if_not_exists = true
    })
    router_space:create_index('id', { parts = { 'id' }, if_not_exists = true })
    router_space:replace({1})
end


local function init(opts)
    if opts.is_master then
        init_router_spaces()
        counter.init_counter_space()
    end
    patch_crud_methods_for_tests()

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

    rawset(_G, 'get_router_name', metadata_utils.get_router_name)

    rawset(_G, 'reset_request_counters', counter.reset_request_counters)
    rawset(_G, 'simple_long_running_function', counter.simple_long_running_function)
    rawset(_G, 'long_running_function', counter.long_running_function)
    rawset(_G, 'get_request_count', counter.get_request_count)

    rawset(_G, 'box_error_unpack_no_connection', box_error_unpack_no_connection)
    rawset(_G, 'box_error_unpack_timeout', box_error_unpack_timeout)
    rawset(_G, 'box_error_timeout', box_error_timeout)
    rawset(_G, 'box_error_non_network_error', box_error_non_network_error)
    rawset(_G, 'crud_error_timeout', crud_error_timeout)
    rawset(_G, 'custom_crud_select', custom_crud_select)
    rawset(_G, 'get_routers_status', get_routers_status)
    rawset(_G, 'init_router_status', init_router_status)
    rawset(_G, 'test_no_such_procedure', test_no_such_procedure)
    rawset(_G, 'get_other_storage_bucket_id', crud_utils.get_other_storage_bucket_id)
    rawset(_G, 'get_composite_data_with_crud', crud_utils.get_composite_data_with_crud)
    rawset(_G, 'select_router_space', select_router_space)
    rawset(_G, 'vshard', vshard)

    create_restricted_user()

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
