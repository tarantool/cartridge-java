package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolClient;
import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.ClusterDiscoveryConfig;
import io.tarantool.driver.cluster.HTTPClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.HTTPDiscoveryClusterAddressProvider;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CartridgeHelper;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.CartridgeHelper.PASSWORD;
import static org.testcontainers.containers.CartridgeHelper.TARANTOOL_ROUTER;
import static org.testcontainers.containers.CartridgeHelper.TARANTOOL_ROUTER_PORT_HTTP;
import static org.testcontainers.containers.CartridgeHelper.USER_NAME;


/**
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
@Testcontainers
public class ClusterDiscoveryIT {

    private static final Logger log = LoggerFactory.getLogger(ClusterDiscoveryIT.class);

    private static final String TEST_ROUTER1_URI = "127.0.0.1:53301";
    private static final String TEST_ROUTER2_URI = "127.0.0.1:53310";

    @BeforeAll
    public static void setUp() throws ExecutionException, InterruptedException {
        CartridgeHelper.environment.start();

        int routerPortHTTP = CartridgeHelper.environment.getServicePort(TARANTOOL_ROUTER, TARANTOOL_ROUTER_PORT_HTTP);
        log.info("Admin interface available on http://127.0.0.1:{}", routerPortHTTP);

        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                CartridgeHelper.getRouterHost(), CartridgeHelper.getRouterPort());
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
        try {
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        client = new StandaloneTarantoolClient(config, serverAddress);
        List<Object> res = client.eval(CartridgeHelper.getAdminBootstrapVshardCmd()).get();
        assertTrue((Boolean) res.get(0));

        try {
            client.close();
        } catch (Exception ignored) {
        }

        CartridgeHelper.environment.waitingFor(TARANTOOL_ROUTER,
                Wait.forLogMessage(".*The cluster is balanced ok.*", 1));
    }

    @AfterAll
    static void destroyDocker() {
        CartridgeHelper.environment.stop();
    }

    @Test
    public void httpClusterDiscovererTest() throws TarantoolClientException {
        HTTPDiscoveryClusterAddressProvider addressProvider = getHttpProvider();
        Collection<TarantoolServerAddress> nodes = addressProvider.getAddresses();

        assertEquals(nodes.size(), 2);
        Set<TarantoolServerAddress> nodeSet = new HashSet<>(nodes);
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER1_URI)));
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER2_URI)));
    }

    private HTTPDiscoveryClusterAddressProvider getHttpProvider() {
        HTTPClusterDiscoveryEndpoint endpoint = new HTTPClusterDiscoveryEndpoint(CartridgeHelper.getHttpDiscoveryURL());

        ClusterDiscoveryConfig config = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        HTTPDiscoveryClusterAddressProvider addressProvider = new HTTPDiscoveryClusterAddressProvider(config);
        return addressProvider;
    }

    @Test
    public void binaryClusterDiscovererTest() {
        TarantoolClusterAddressProvider addressProvider = getBinaryProvider();

        Collection<TarantoolServerAddress> nodes = addressProvider.getAddresses();
        assertEquals(nodes.size(), 2);
        Set<TarantoolServerAddress> nodeSet = new HashSet<>(nodes);
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER1_URI)));
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER2_URI)));
    }

    private TarantoolClusterAddressProvider getBinaryProvider() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withCredentials(credentials)
                .withEntryFunction("get_routers")
                .withServerAddress(new TarantoolServerAddress(
                        CartridgeHelper.getRouterHost(), CartridgeHelper.getRouterPort()))
                .build();

        ClusterDiscoveryConfig clusterDiscoveryConfig = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .withDelay(1)
                .build();

        BinaryDiscoveryClusterAddressProvider addressProvider =
                new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig);

        return addressProvider;
    }

    @Test
    public void connectWithBinaryClusterDiscovery() throws TarantoolClientException {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        ClusterTarantoolClient client = new ClusterTarantoolClient(
                config, TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory.INSTANCE, getBinaryProvider());

        assertNotNull(client.getVersion(), "Version must not be null");
        assertTrue(client.getVersion().toString().contains("Tarantool"), "Version must contain Tarantool");
    }

    @Test
    public void connectWithHttpClusterDiscovery() throws TarantoolClientException {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        ClusterTarantoolClient client = new ClusterTarantoolClient(
                config, TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory.INSTANCE, getHttpProvider());

        assertNotNull(client.getVersion(), "Version must not be null");
        assertTrue(client.getVersion().toString().contains("Tarantool"), "Version must contain Tarantool");
    }
}