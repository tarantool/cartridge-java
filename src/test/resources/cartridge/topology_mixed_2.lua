cartridge = require('cartridge')
replicasets = { {
                    alias = 'mixed-replicasets',
                    roles = { 'vshard-router', 'app.roles.api_router', 'vshard-storage', 'app.roles.api_storage', 'app.roles.custom' },
                    join_servers = {
                        { uri = 'localhost:3351' },
                        { uri = 'localhost:3352' },
                        { uri = 'localhost:3353' },
                    }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
