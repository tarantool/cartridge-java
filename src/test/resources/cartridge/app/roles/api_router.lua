local vshard = require('vshard')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

-- function to get cluster schema
local function crud_get_schema()
    local replicaset = select(2, next(vshard.router.routeall()))
    local uniq_spaces = {}
    local spaces_ids = {}
    for _, space in pairs(replicaset.master.conn.space) do
        
        if (spaces_ids[space.id] == nil) then
            local space_copy = {
                engine = space.engine,
                field_count = space.field_count,
                id = space.id,
                name = space.name,
                index = {},
                _format = space._format,
            }

            for i, space_index in pairs(space.index) do
                if type(i) == 'number' then
                    local index_copy = {
                        id = space_index.id,
                        name = space_index.name,
                        unique = space_index.unique,
                        type = space_index.type,
                        parts = space_index.parts,
                    }
                    table.insert(space_copy.index, index_copy)
                end
            end

            table.insert(uniq_spaces, {space_copy} )
            spaces_ids[space.id] = true
        end
    end
    return uniq_spaces
end

local function truncate_space(space_name)
    local storages = cartridge_rpc.get_candidates('app.roles.api_storage')
    cartridge_pool.map_call('box.schema.space[' .. space_name .. ']:truncate', nil, {uri_list = storages})
end

local function init(opts)

    rawset(_G, 'crud_get_schema', crud_get_schema)
    rawset(_G, 'truncate_space', truncate_space)

    return true
end

return {
    role_name = 'app.roles.api_router',
    init = init,
    dependencies = {
        'cartridge.roles.crud-router',
    }
}
