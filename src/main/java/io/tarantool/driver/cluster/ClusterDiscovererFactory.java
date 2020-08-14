package io.tarantool.driver.cluster;

/**
 * Factory for {@link AddressProvider} instances.
 *
 * @author Sergey Volgin
 */
public final class ClusterDiscovererFactory {

    public static ClusterDiscoverer create(ServerSelectStrategy serverSelectStrategy, ClusterDiscoveryConfig config) {
        ClusterDiscoverer clusterDiscoverer;

        if (config.getEndpoint() instanceof TarantoolClusterDiscoveryEndpoint) {
            clusterDiscoverer = new TarantoolClusterDiscoverer(serverSelectStrategy, config);
        } else {
            clusterDiscoverer = new HTTPClusterDiscoverer(serverSelectStrategy, config);
        }

        return clusterDiscoverer;
    }

    private ClusterDiscovererFactory() {
    }
}
