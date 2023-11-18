local crud = require('crud')
local uuid = require('uuid')

crud.truncate('test_space')
crud.truncate('test__profile')

for i = 1, 10000 do
    crud.insert('test_space', {1000000 + i, nil, uuid.new(), 200000 + i})
    crud.insert('test__profile', {1000000 + i, nil, uuid.new(), 50000 + i, 100000 + i})
end
