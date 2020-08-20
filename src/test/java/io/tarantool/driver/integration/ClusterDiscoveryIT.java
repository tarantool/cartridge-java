package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.ClusterDiscoveryConfig;
import io.tarantool.driver.cluster.HTTPClusterAddressProvider;
import io.tarantool.driver.cluster.HTTPClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.RoundRobinAddressProvider;
import io.tarantool.driver.cluster.SimpleAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryEndpoint;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CartridgeHelper;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.testcontainers.containers.CartridgeHelper.PASSWORD;
import static org.testcontainers.containers.CartridgeHelper.TARANTOOL_ROUTER;
import static org.testcontainers.containers.CartridgeHelper.TARANTOOL_ROUTER_PORT;
import static org.testcontainers.containers.CartridgeHelper.TARANTOOL_ROUTER_PORT_HTTP;
import static org.testcontainers.containers.CartridgeHelper.USER_NAME;


@Testcontainers
public class ClusterDiscoveryIT {

    private static final Logger log = LoggerFactory.getLogger(ClusterDiscoveryIT.class);

    private static String ROUTER_HOST;
    private static int ROUTER_PORT;

    @BeforeAll
    public static void setUp() throws ExecutionException, InterruptedException {
        CartridgeHelper.environment.start();

        int routerPortHTTP = CartridgeHelper.environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP);
        log.info("Admin interface available on http://127.0.0.1:{}", routerPortHTTP);

        ROUTER_HOST = CartridgeHelper.environment.getServiceHost(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);
        ROUTER_PORT = CartridgeHelper.environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT);

        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(5)
                .withRequestTimeout(5)
                .withReadTimeout(5)
                .withHost(ROUTER_HOST, ROUTER_PORT)
                .build();

        TarantoolClient client = new StandaloneTarantoolClient(config);
        TarantoolConnection connection;
        try {
            connection = client.connect();
        } catch (Exception e) {
            log.error("connect error", e);
            connection = client.connect();
        }

        String cmd = CartridgeHelper.getAdminEditTopologyCmd();

        try {
            connection.eval(cmd).get();
        } catch (ExecutionException ignored) {
            //it's is ok if disconnected
        }

        try {
            connection.close();
        } catch (Exception ignored) {
        }

        //reconnect
        connection = client.connect();
        List<Object> res = connection.eval(CartridgeHelper.getAdminBootstrapVshardCmd()).get();
        assertTrue((Boolean) res.get(0));
    }

    @AfterAll
    static void destroyDocker() {
        CartridgeHelper.environment.stop();
    }

    @Test
    public void httpClusterDiscovererTest() throws TarantoolClientException {

        SimpleAddressProvider simpleAddressProvider =
                new RoundRobinAddressProvider(Collections.singletonList(new TarantoolServerAddress()));
        HTTPClusterDiscoveryEndpoint endpoint = new HTTPClusterDiscoveryEndpoint(CartridgeHelper.getHttpDiscoveryURL());

        ClusterDiscoveryConfig config = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        HTTPClusterAddressProvider addressProvider = new HTTPClusterAddressProvider(simpleAddressProvider, config);
        List<TarantoolServerAddress> nodes = addressProvider.getNodes();

        assertEquals(nodes.size(), 2);
    }

    @Test
    public void tarantoolClusterDiscovererTest() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolClusterDiscoveryEndpoint endpoint = new TarantoolClusterDiscoveryEndpoint.Builder()
                .withCredentials(credentials)
                .withEntryFunction("get_replica_set")
                .withServerAddress(new TarantoolServerAddress(ROUTER_HOST, ROUTER_PORT))
                .build();

        ClusterDiscoveryConfig clusterDiscoveryConfig = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        SimpleAddressProvider simpleAddressProvider =
                new RoundRobinAddressProvider(Collections.singletonList(new TarantoolServerAddress()));
        TarantoolClusterAddressProvider addressProvider =
                new TarantoolClusterAddressProvider(simpleAddressProvider, clusterDiscoveryConfig);

        List<TarantoolServerAddress> nodes = addressProvider.getNodes();
        assertTrue(nodes.size() > 1);
    }

//    @Test
//    public void connectWithHttpServiceDiscovery() throws TarantoolClientException {
//        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
//        HTTPClusterDiscoveryEndpoint endpoint =
//                  new HTTPClusterDiscoveryEndpoint(CartridgeHelper.getHttpDiscoveryURL());
//
//        ClusterDiscoveryConfig clusterDiscoveryConfig = new ClusterDiscoveryConfig.Builder()
//                .withEndpoint(endpoint).build();
//
//        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
//                .withCredentials(credentials)
//                .withConnectTimeout(1000 * 5)
//                .withReadTimeout(1000 * 5)
//                .withRequestTimeout(1000 * 5)
//                .withHost(ROUTER_HOST, ROUTER_PORT)
//                .withClusterDiscoveryConfig(clusterDiscoveryConfig)
//                .build();
//
//        StandaloneTarantoolClient client = new StandaloneTarantoolClient(config);
//
//        TarantoolConnection connection = client.connect();
//        assertEquals("localhost/127.0.0.1:3302", connection.getChannel().remoteAddress().toString());
//        connection = client.connect();
//        assertEquals("localhost/127.0.0.1:3304", connection.getChannel().remoteAddress().toString());
//        connection = client.connect();
//        assertEquals("localhost/127.0.0.1:3302", connection.getChannel().remoteAddress().toString());
//    }
}
