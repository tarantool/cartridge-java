local vshard = require('vshard')

local function get_other_storage_bucket_id(key)
    -- from tarantool/crud/test/helper.lua
    local bucket_id = vshard.router.bucket_id_strcrc32(key)

    local replicasets = vshard.router.routeall()
    local other_replicaset_uuid
    for replicaset_uuid, replicaset in pairs(replicasets) do
        local _, err = replicaset:callrw('vshard.storage.bucket_stat', { bucket_id })
        if err ~= nil and err.name == 'WRONG_BUCKET' then
            other_replicaset_uuid = replicaset_uuid
            break
        end
        if err ~= nil then
            return nil, string.format(
                'vshard.storage.bucket_stat returned unexpected error: %s',
                require('json').encode(err)
            )
        end
    end
    if other_replicaset_uuid == nil then
        return nil, 'Other replicaset is not found'
    end
    local other_replicaset = replicasets[other_replicaset_uuid]
    if other_replicaset == nil then
        return nil, string.format('Replicaset %s not found', other_replicaset_uuid)
    end
    local buckets_info = other_replicaset:callrw('vshard.storage.buckets_info')
    local res_bucket_id = next(buckets_info)
    return res_bucket_id
end

local function get_composite_data_with_crud(id)
    local test_space = crud.get("test_space", id)
    local test_space_to_join = crud.get("test_space_to_join", id)

    table.move(
        test_space_to_join.metadata,
        1,
        #test_space_to_join.metadata,
        #test_space.metadata + 1,
        test_space.metadata)
    table.move(
        test_space_to_join.rows[1],
        1,
        #test_space_to_join.rows[1],
        #test_space.rows[1] + 1,
        test_space.rows[1]
    )
    return test_space
end

return {
    get_other_storage_bucket_id = get_other_storage_bucket_id,
    get_composite_data_with_crud = get_composite_data_with_crud,
}
