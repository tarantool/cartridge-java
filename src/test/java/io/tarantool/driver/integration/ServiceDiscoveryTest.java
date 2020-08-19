package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
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
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ServiceDiscoveryTest {

    private static String DEFAULT_HOST = "127.0.0.1";
    private static int DEFAULT_PORT = 3301;
    private static String USER_NAME = "admin";
    private static String PASSWORD = "myapp-cluster-cookie";
    private static String HTTP_DISCOVERY_URI = "http://" + DEFAULT_HOST + ":8081/endpoints";

    @Test
    public void httpClusterDiscovererTest() throws TarantoolClientException {
        SimpleAddressProvider simpleAddressProvider = new RoundRobinAddressProvider(Collections.singletonList(new TarantoolServerAddress()));
        HTTPClusterDiscoveryEndpoint endpoint = new HTTPClusterDiscoveryEndpoint(HTTP_DISCOVERY_URI);

        ClusterDiscoveryConfig config = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        HTTPClusterAddressProvider addressProvider = new HTTPClusterAddressProvider(simpleAddressProvider, config);
        List<TarantoolServerAddress> nodes = addressProvider.getNodes();

        assertTrue(nodes.size() > 1);
    }

    @Test
    public void tarantoolClusterDiscovererTest() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        TarantoolClusterDiscoveryEndpoint endpoint = new TarantoolClusterDiscoveryEndpoint.Builder()
                .withCredentials(credentials)
                .withEntryFunction("get_replica_set")
                .withServerAddress(new TarantoolServerAddress(DEFAULT_HOST, DEFAULT_PORT))
                .build();

        ClusterDiscoveryConfig clusterDiscoveryConfig = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5)
                .withConnectTimeout(1000 * 5)
                .build();

        SimpleAddressProvider simpleAddressProvider = new RoundRobinAddressProvider(Collections.singletonList(new TarantoolServerAddress()));
        TarantoolClusterAddressProvider addressProvider = new TarantoolClusterAddressProvider(simpleAddressProvider, clusterDiscoveryConfig);

        List<TarantoolServerAddress> nodes = addressProvider.getNodes();
        assertTrue(nodes.size() > 1);
    }

    @Test
    public void connectWithHttpServiceDiscovery() throws TarantoolClientException {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);
        HTTPClusterDiscoveryEndpoint endpoint = new HTTPClusterDiscoveryEndpoint(HTTP_DISCOVERY_URI);

        ClusterDiscoveryConfig clusterDiscoveryConfig = new ClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint).build();

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 500)
                .withReadTimeout(1000 * 500)
                .withRequestTimeout(1000 * 500)
                .withHost(DEFAULT_HOST, DEFAULT_PORT)
                .withClusterDiscoveryConfig(clusterDiscoveryConfig)
                .build();

        StandaloneTarantoolClient client = new StandaloneTarantoolClient(config);

        TarantoolConnection connection = client.connect();
        assertEquals("localhost/127.0.0.1:3302", connection.getChannel().remoteAddress().toString());
        connection = client.connect();
        assertEquals("localhost/127.0.0.1:3304", connection.getChannel().remoteAddress().toString());
        connection = client.connect();
        assertEquals("localhost/127.0.0.1:3302", connection.getChannel().remoteAddress().toString());
    }
}
