cartridge = require('cartridge')
replicasets = { {
                    alias = 'mixed-replicasets',
                    roles = { 'vshard-router', 'app.roles.api_router', 'vshard-storage', 'app.roles.api_storage', 'app.roles.custom' },
                    join_servers = {
                        { uri = '0.0.0.0:3301' },
                        { uri = '0.0.0.0:3302' },
                        { uri = '0.0.0.0:3303' },
                    }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
