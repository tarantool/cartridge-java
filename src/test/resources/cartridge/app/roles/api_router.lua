local cartridge = require('cartridge')
local vshard = require('vshard')
local crud = require('crud')

-- CRUD functions wrappers
local function crud_get(space_name, key, opts)
    return crud.get(space_name, key, opts)
end

local function crud_insert(space_name, obj, opts)
    return crud.insert(space_name, obj, opts)
end

local function crud_delete(space_name, key, opts)
    return crud.delete(space_name, key, opts)
end

local function crud_replace(space_name, obj, opts)
    return crud.replace(space_name, obj, opts)
end

local function crud_update(space_name, key, operations, opts)
    return crud.update(space_name, key, operations, opts)
end

local function crud_upsert(space_name, obj, operations, opts)
    return crud.upsert(space_name, obj, operations, opts)
end

local function crud_select(space_name, user_conditions, opts)
    return crud.select(space_name, user_conditions, opts)
end

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

            for i = 0, #space.index do
                local space_index = space.index[i]
                local index_copy = {
                    id = space_index.id,
                    name = space_index.name,
                    unique = space_index.unique,
                    type = space_index.type,
                    parts = space_index.parts,
                }
                table.insert(space_copy.index, index_copy)
            end

            table.insert(uniq_spaces, {space_copy} )
            spaces_ids[space.id] = true
        end
    end
    return uniq_spaces
end


local function init(opts)
    if opts.is_master then
    end

    rawset(_G, 'crud_get', crud_get)
    rawset(_G, 'crud_insert', crud_insert)
    rawset(_G, 'crud_delete', crud_delete)
    rawset(_G, 'crud_replace', crud_replace)
    rawset(_G, 'crud_update', crud_update)
    rawset(_G, 'crud_upsert', crud_upsert)
    rawset(_G, 'crud_select', crud_select)

    rawset(_G, 'crud_get_schema', crud_get_schema)

    return true
end

return {
    role_name = 'app.roles.api_router',
    init = init,
    dependencies = {
        'cartridge.roles.vshard-router'
    }
}
