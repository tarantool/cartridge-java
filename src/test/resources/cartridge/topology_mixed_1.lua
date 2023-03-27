cartridge = require('cartridge')
replicasets = { {
                    alias = 'mixed-replicasets',
                    roles = { 'vshard-router', 'app.roles.api_router', 'vshard-storage', 'app.roles.api_storage', 'app.roles.custom' },
                    join_servers = {
                        { uri = 'localhost:3341' },
                        { uri = 'localhost:3342' },
                        { uri = 'localhost:3343' },
                    }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
