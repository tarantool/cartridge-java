cartridge = require('cartridge')
replicasets = { {
                    alias = 'app-router',
                    roles = { 'vshard-router', 'app.roles.api_router', 'vshard-storage', 'app.roles.api_storage', 'app.roles.custom' },
                    join_servers = {
                        { uri = 'localhost:3301' },
                        { uri = 'localhost:3302' },
                        { uri = 'localhost:3311' },
                        { uri = 'localhost:3312' },
                    }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
