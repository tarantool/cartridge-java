package io.tarantool.driver.cluster;

/**
 * Factory for {@link AddressProvider} instances.
 *
 * @author Sergey Volgin
 */
public final class AddressProviderFactory {

    public static AddressProvider create(SimpleAddressProvider simpleAddressProvider,
                                                ClusterDiscoveryConfig config) {

        if (config == null) {
            return simpleAddressProvider;
        }

        AddressProvider clusterAddressProvider;
        if (config.getEndpoint() instanceof TarantoolClusterDiscoveryEndpoint) {
            clusterAddressProvider = new TarantoolClusterAddressProvider(simpleAddressProvider, config);
        } else {
            clusterAddressProvider = new HTTPClusterAddressProvider(simpleAddressProvider, config);
        }

        return clusterAddressProvider;
    }

    private AddressProviderFactory() {
    }
}
