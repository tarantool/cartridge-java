package org.testcontainers.containers;

import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

/**
 * @author Sergey Volgin
 */
public class CartridgeHelper {

    private static final Logger log = LoggerFactory.getLogger(CartridgeHelper.class);

    public static String TARANTOOL_ROUTER = "tarantool.router_1";
    public static int TARANTOOL_ROUTER_PORT = 3301;
    public static int TARANTOOL_ROUTER_PORT_HTTP = 8081;
    public static String TARANTOOL_S1_MASTER = "tarantool.s1master_1";
    public static int TARANTOOL_S1_MASTER_PORT = 3302;
    public static String TARANTOOL_S1_REPLICA = "tarantool.s1replica_1";
    public static int TARANTOOL_S1_REPLICA_PORT = 3303;
    public static String TARANTOOL_S2_MASTER = "tarantool.s2master_1";
    public static int TARANTOOL_S2_MASTER_PORT = 3304;
    public static String TARANTOOL_S2_REPLICA = "tarantool.s2replica_1";
    public static int TARANTOOL_S2_REPLICA_PORT = 3305;

    public static String USER_NAME = "admin";
    public static String PASSWORD = "myapp-cluster-cookie";

    @ClassRule
    public static DockerComposeContainer<?> environment = new DockerComposeContainer(
            new File("src/test/resources/org/testcontainers/containers/cartridge-compose.yml"))
            .withLogConsumer(TARANTOOL_ROUTER, new Slf4jLogConsumer(log))
            .waitingFor(TARANTOOL_ROUTER, Wait.forLogMessage(".*entering the event loop.*", 1))
            .withExposedService(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT)
            .withExposedService(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP)
            .withExposedService(TARANTOOL_S1_MASTER, TARANTOOL_S1_MASTER_PORT)
            .withExposedService(TARANTOOL_S1_REPLICA, TARANTOOL_S1_REPLICA_PORT)
            .withExposedService(TARANTOOL_S2_MASTER, TARANTOOL_S2_MASTER_PORT)
            .withExposedService(TARANTOOL_S2_REPLICA, TARANTOOL_S2_REPLICA_PORT)
            .withTailChildContainers(true)
            .withLocalCompose(true);

    public static String getHttpDiscoveryURL() {
        int routerPortHTTP = environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP);
        String host = environment.getServiceHost(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
        return "http://" + host + ":" + routerPortHTTP + "/endpoints";
    }

    public static String getAdminEditTopologyCmd() {
        return "cartridge = require('cartridge')\n " +
                "replicasets = {{" +
                "    alias = 'app-router'," +
                "    roles = {'vshard-router', 'app.roles.custom'}," +
                "    join_servers = {{uri = '172.16.237.101:3301'}}," +
                "}," +
                "{" +
                "    alias = 's1-storage'," +
                "    roles = {'vshard-storage', 'app.roles.storage'}," +
                "    join_servers = {{uri = '172.16.237.102:3302'}, {uri = '172.16.237.103:3303'}}," +
                "}," +
                "{" +
                "    alias = 's2-storage'," +
                "    roles = {'vshard-storage', 'app.roles.storage'}," +
                "    join_servers = {{uri = '172.16.237.104:3304'}, {uri = '172.16.237.105:3305'}}," +
                "}}\n " +
                "return cartridge.admin_edit_topology({replicasets = replicasets})";
    }

    public static String getAdminBootstrapVshardCmd() {
        return "return require('cartridge').admin_bootstrap_vshard()";
    }

    private CartridgeHelper() {

    }
}
