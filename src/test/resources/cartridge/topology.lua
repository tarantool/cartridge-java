cartridge = require('cartridge')
replicasets = { {
                    alias = 'app-router',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3301' } }
                }, {
                    alias = 'app-router-second',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3311' } }
                }, {
                    alias = 'app-router-third',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3312' } }
                }, {
                    alias = 's1-storage',
                    roles = { 'vshard-storage', 'app.roles.api_storage' },
                    join_servers = { { uri = 'localhost:3302' } }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
