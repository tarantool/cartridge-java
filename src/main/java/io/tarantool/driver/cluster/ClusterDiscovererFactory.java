package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.exceptions.TarantoolClientException;

/**
 * Factory for {@link ClusterDiscoverer} instances.
 *
 * @author Sergey Volgin
 */
public class ClusterDiscovererFactory {

    public static ClusterDiscoverer create(ClusterDiscoveryEndpoint endpoint,
                                           TarantoolClient client, TarantoolClientConfig config) {
        ClusterDiscoverer clusterDiscoverer;
        if (endpoint instanceof TarantoolClusterDiscoveryEndpoint) {
            clusterDiscoverer = new TarantoolClusterDiscoverer(client, (TarantoolClusterDiscoveryEndpoint) endpoint);
        } else if (endpoint instanceof HTTPClusterDiscoveryEndpoint) {
            clusterDiscoverer = new HTTPClusterDiscoverer((HTTPClusterDiscoveryEndpoint) endpoint, config.getConnectTimeout());
        } else {
            throw new TarantoolClientException("Unsupported service discovery type.");
        }
        return clusterDiscoverer;
    }

    private ClusterDiscovererFactory() {
    }
}
