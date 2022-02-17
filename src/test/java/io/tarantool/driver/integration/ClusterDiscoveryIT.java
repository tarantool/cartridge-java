package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.HTTPClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.HTTPDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public class ClusterDiscoveryIT extends SharedCartridgeContainer {

    private static final String TEST_ROUTER1_URI = "localhost:3301";
    private static final String TEST_ROUTER2_URI = "localhost:3311";

    @BeforeAll
    public static void setUp() {
        startCluster();
    }

    @Test
    public void httpClusterDiscovererTest() throws TarantoolClientException {
        HTTPDiscoveryClusterAddressProvider addressProvider = getHttpProvider();
        Collection<TarantoolServerAddress> nodes = addressProvider.getAddresses();

        assertEquals(nodes.size(), 3);
        Set<TarantoolServerAddress> nodeSet = new HashSet<>(nodes);
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER1_URI)));
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER2_URI)));
    }

    private HTTPDiscoveryClusterAddressProvider getHttpProvider() {
        String discoveryAddress = "http://" + container.getAPIHost() + ":" + container.getAPIPort() + "/routers";
        HTTPClusterDiscoveryEndpoint endpoint = new HTTPClusterDiscoveryEndpoint.Builder()
                .withURI(discoveryAddress)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        TarantoolClusterDiscoveryConfig config = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .build();

        return new HTTPDiscoveryClusterAddressProvider(config);
    }

    @Test
    public void binaryClusterDiscovererTest() {
        TarantoolClusterAddressProvider addressProvider = getBinaryProvider();

        Collection<TarantoolServerAddress> nodes = addressProvider.getAddresses();
        assertEquals(nodes.size(), 3);
        Set<TarantoolServerAddress> nodeSet = new HashSet<>(nodes);
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER1_URI)));
        assertTrue(nodeSet.contains(new TarantoolServerAddress(TEST_ROUTER2_URI)));
    }

    private TarantoolClusterAddressProvider getBinaryProvider() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                container.getUsername(), container.getPassword());

        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(credentials)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withClientConfig(config)
                .withEntryFunction("get_routers")
                .withEndpointProvider(() -> Collections.singletonList(
                        new TarantoolServerAddress(container.getRouterHost(), container.getRouterPort())))
                .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withDelay(1)
                .build();

        return new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig);
    }

    @Test
    public void connectWithBinaryClusterDiscovery() throws TarantoolClientException {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(container.getUsername(), container.getPassword()))
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(
                config,
                new TestWrappedClusterAddressProvider(getBinaryProvider(), container));

        assertNotNull(client.getVersion(), "Version must not be null");
        assertTrue(client.getVersion().toString().contains("Tarantool"), "Version must contain Tarantool");
    }

    @Test
    public void connectWithHttpClusterDiscovery() throws TarantoolClientException {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(container.getUsername(), container.getPassword()))
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(
                config,
                new TestWrappedClusterAddressProvider(getHttpProvider(), container));

        assertNotNull(client.getVersion(), "Version must not be null");
        assertTrue(client.getVersion().toString().contains("Tarantool"), "Version must contain Tarantool");
    }
}
