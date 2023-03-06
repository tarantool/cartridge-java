local M = {}

local function tarantool_version()
    local major_minor_patch = _G._TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    return major, minor, patch
end
M.tarantool_version = tarantool_version

local function get_field_type_by_version()
    local tarantoolVersion = box.info.version
    --todo: change this solution for more generic cases like for 10+ versions
    local version = tonumber(string.sub(tarantoolVersion, 1, 3))

    if version >= 2.3 then
        return 'double'
    end

    return 'number'
end
M.get_field_type_by_version = get_field_type_by_version

return M
