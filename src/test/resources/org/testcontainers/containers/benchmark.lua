box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024, -- 128 Mb
    -- log = 'file:/tmp/tarantool.log',
    log_level = 6,
}
-- API user will be able to login with this password
box.schema.user.create('api_user', { password = 'secret' })
-- API user will be able to create spaces, add or remove data, execute functions
box.schema.user.grant('api_user', 'read,write,execute', 'universe')

local test_space = box.schema.space.create(
        'test_space',
        {
            format = {
                { 'field1', 'unsigned' },
                { 'field2'  },
                { 'field3'  },
                { 'field4'  },
                { 'field5'  },
                { 'field6'  },
                { 'field7'  },
                { 'field8'  },
                { 'field9'  },
                { 'field10' },
                { 'field11' },
                { 'field12' },
                { 'field13' },
                { 'field14' },
                { 'field15' },
            },
            if_not_exists = true,
        }
)
test_space:create_index('field1', { parts = { 'field1' }, if_not_exists = true, })

local tuples = {}
local tuples_count = 1000
for i = 1, tuples_count do
    table.insert(tuples, {
        i,                                  -- field1
        'aaaaaaaa',                         -- field2
        box.NULL,                           -- field3
        'bbbbb',                            -- field4
        false,                              -- field5
        99,                                 -- field6
        'cccccc',                           -- field7
        3.4654,                             -- field8
        'a',                                -- field9
        -123312,                            -- field10
        { 0, "asdsad", 1, false, 2.2, },    -- field11
        {hello = "world", d=3},             -- field12
        true,                               -- field13
        9223372036854775807ULL,             -- field14
        -9223372036854775807LL              -- field15
    })
end

function return_arrays_with_different_types()
    return tuples
end
