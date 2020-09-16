package io.tarantool.driver;

import io.tarantool.driver.cluster.BinaryClusterDiscoveryEndpoint;
import io.tarantool.driver.cluster.BinaryDiscoveryClusterAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryConfig;
import io.tarantool.driver.cluster.HTTPDiscoveryClusterAddressProvider;
import org.springframework.util.Assert;

import java.util.Collection;

/**
 * Factory for Tarantool address provider instances.
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public final class TarantoolAddressProviderFactory {

    private TarantoolAddressProviderFactory() {
    }

    public TarantoolClusterAddressProvider createClusterAddressProvider(Collection<TarantoolServerAddress> nodes) {
        Assert.notNull(nodes, "Collection of Tarantool server address should not be null");
        Assert.notEmpty(nodes, "Collection of Tarantool server address should not be empty");

        return () -> nodes;
    }

    public TarantoolClusterAddressProvider createClusterAddressProviderWithDiscovery(
            Collection<TarantoolServerAddress> nodes, TarantoolClusterDiscoveryConfig config) {
        TarantoolClusterAddressProvider addressProvider;

        if (config != null) {
            if (config.getEndpoint() instanceof BinaryClusterDiscoveryEndpoint) {
                addressProvider = new BinaryDiscoveryClusterAddressProvider(config);
            } else {
                addressProvider = new HTTPDiscoveryClusterAddressProvider(config);
            }
        } else {
            addressProvider = createClusterAddressProvider(nodes);
        }

        return addressProvider;
    }
}
