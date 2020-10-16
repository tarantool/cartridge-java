package io.tarantool.driver.integration;

import io.tarantool.driver.ClusterTarantoolClient;
import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.TestWrappedClusterAddressProvider;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies;
import org.junit.ClassRule;
import org.testcontainers.containers.TarantoolCartridgeContainer;

abstract class SharedCartridgeContainer {

    protected static final int DEFAULT_TEST_TIMEOUT = 5 * 1000;

    protected static void startCluster() {
        if (!container.isRunning()) {
            container.start();
        }
    }

    @ClassRule
    protected static final TarantoolCartridgeContainer container = new TarantoolCartridgeContainer(
            //"tarantool/tarantool:2.x-centos7",
            //"tarantool/cartridge-driver-test",
            "cartridge/instances.yml",
            "cartridge/topology.lua")
            .withDirectoryBinding("cartridge")
            .cleanUpDirectory("cartridge/tmp")
            .withReuse(true);

    protected static TarantoolClusterAddressProvider getClusterAddressProvider() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                container.getUsername(), container.getPassword());

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withCredentials(credentials)
                .withEntryFunction("get_routers")
                .withServerAddress(new TarantoolServerAddress(
                        container.getRouterHost(), container.getRouterPort()))
                .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(DEFAULT_TEST_TIMEOUT)
                .withConnectTimeout(DEFAULT_TEST_TIMEOUT)
                .withDelay(1)
                .build();

        return new TestWrappedClusterAddressProvider(
                new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig),
                container);
    }

    protected static ProxyTarantoolClient createClusterClient() {
        TarantoolCredentials credentials =
                new SimpleTarantoolCredentials(container.getUsername(), container.getPassword());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(DEFAULT_TEST_TIMEOUT)
                .withReadTimeout(DEFAULT_TEST_TIMEOUT)
                .withRequestTimeout(DEFAULT_TEST_TIMEOUT)
                .build();

        ClusterTarantoolClient clusterClient =
                new ClusterTarantoolClient(config, getClusterAddressProvider(),
                        TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory.INSTANCE);

        return new ProxyTarantoolClient(clusterClient);
    }
}
