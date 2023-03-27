cartridge = require('cartridge')
replicasets = { {
                    alias = 'app-router',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3311' } }
                }, {
                    alias = 'app-router-second',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3312' } }
                }, {
                    alias = 'app-router-third',
                    roles = { 'vshard-router', 'app.roles.custom', 'app.roles.api_router' },
                    join_servers = { { uri = 'localhost:3313' } }
                }, {
                    alias = 's1-storage',
                    roles = { 'vshard-storage', 'app.roles.api_storage' },
                    join_servers = { { uri = 'localhost:3314' } }
                },
                {
                    alias = 's2-storage',
                    roles = { 'vshard-storage', 'app.roles.api_storage' },
                    join_servers = { { uri = 'localhost:3315' } }
                } }
return cartridge.admin_edit_topology({ replicasets = replicasets })
