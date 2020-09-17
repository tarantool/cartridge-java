package org.testcontainers.containers;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Sergey Volgin
 */
public final class CartridgeHelper {

    private static final Logger log = LoggerFactory.getLogger(CartridgeHelper.class);

    public static String TARANTOOL_ROUTER = "tarantool.router_1";
    public static int TARANTOOL_ROUTER_PORT = 3301;
    public static String TARANTOOL_ROUTER_2 = "tarantool.router2_1";
    public static int TARANTOOL_ROUTER_2_PORT = 3310;
    public static int TARANTOOL_ROUTER_PORT_HTTP = 8081;
    public static String TARANTOOL_S1_MASTER = "tarantool.s1master_1";
    public static int TARANTOOL_S1_MASTER_PORT = 3302;
    public static String TARANTOOL_S2_MASTER = "tarantool.s2master_1";
    public static int TARANTOOL_S2_MASTER_PORT = 3304;

    public static String USER_NAME = "admin";
    public static String PASSWORD = "myapp-cluster-cookie";

    @ClassRule
    public static DockerComposeContainer<?> environment = new DockerComposeContainer(
            new File("src/test/resources/org/testcontainers/containers/cartridge-compose.yml"))
            .withLogConsumer(TARANTOOL_ROUTER, new Slf4jLogConsumer(log))
            .waitingFor(TARANTOOL_ROUTER, Wait.forLogMessage(".*entering the event loop.*", 1))
            .withExposedService(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT)
            .withExposedService(TARANTOOL_ROUTER_2, TARANTOOL_ROUTER_2_PORT)
            .withExposedService(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP)
            .withExposedService(TARANTOOL_S1_MASTER, TARANTOOL_S1_MASTER_PORT)
            .withExposedService(TARANTOOL_S2_MASTER, TARANTOOL_S2_MASTER_PORT)
            .withTailChildContainers(true)
            .withLocalCompose(true);

    public static String getHttpDiscoveryURL() {
        int routerPortHTTP = environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP);
        String host = environment.getServiceHost(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
        return "http://" + host + ":" + routerPortHTTP + "/routers";
    }

    public static String getRouterHost() {
        return CartridgeHelper.environment.getServiceHost(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
    }

    public static int getRouterPort() {
        return CartridgeHelper.environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
    }

    public static String getAdminEditTopologyCmd() {
        return "cartridge = require('cartridge')\n " +
                "replicasets = {{" +
                "    alias = 'app-router'," +
                "    roles = {'vshard-router', 'app.roles.custom', 'app.roles.api_router'}," +
                "    join_servers = {{uri = '172.16.237.101:3301'}}," +
                "}, {" +
                "    alias = 'app-router2'," +
                "    roles = {'vshard-router', 'app.roles.custom', 'app.roles.api_router'}," +
                "    join_servers = {{uri = '172.16.237.110:3310'}}," +
                "}, {" +
                "    alias = 's1-storage'," +
                "    roles = {'vshard-storage', 'app.roles.storage', 'app.roles.api_storage'}," +
                "    join_servers = {{uri = '172.16.237.102:3302'}}," +
                "}, {" +
                "    alias = 's2-storage'," +
                "    roles = {'vshard-storage', 'app.roles.storage', 'app.roles.api_storage'}," +
                "    join_servers = {{uri = '172.16.237.104:3304'}}," +
                "}}\n " +
                "return cartridge.admin_edit_topology({replicasets = replicasets})";
    }

    public static String getAdminBootstrapVshardCmd() {
        return "return require('cartridge').admin_bootstrap_vshard()";
    }

    public static List<Object> startCluster() throws ExecutionException, InterruptedException {
        CartridgeHelper.environment.start();
        final String routerHost = CartridgeHelper.environment.getServiceHost(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
        final int routerPort = CartridgeHelper.environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(routerHost, routerPort);

        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .build();

        TarantoolClient client = new StandaloneTarantoolClient(config, serverAddress);

        String cmd = CartridgeHelper.getAdminEditTopologyCmd();

        try {
            client.eval(cmd).get();
            //the connections will be closed after that command
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // The client must reconnect automatically
        return client.eval(CartridgeHelper.getAdminBootstrapVshardCmd()).get();
    }

    private CartridgeHelper() {
    }
}
