package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;

import java.util.Collection;

/**
 * Implementation of {@link AbstractTarantoolConnectionManager}, aware of connecting to the Tarantool cluster
 *
 * @author Alexey Kuzin
 */
public class TarantoolClusterConnectionManager extends AbstractTarantoolConnectionManager {
    private final TarantoolClusterAddressProvider addressProvider;

    /**
     * Basic constructor.
     *
     * @param config client configuration
     * @param connectionFactory manages instantiation of Tarantool server connections
     * @param listeners are invoked after connection is established
     * @param addressProvider provides Tarantool server nodes addresses
     */
    public TarantoolClusterConnectionManager(TarantoolClientConfig config,
                                             TarantoolConnectionFactory connectionFactory,
                                             TarantoolConnectionListeners listeners,
                                             TarantoolClusterAddressProvider addressProvider) {
        super(config, connectionFactory, listeners);
        this.addressProvider = addressProvider;
    }

    @Override
    protected Collection<TarantoolServerAddress> getAddresses() {
        return addressProvider.getAddresses();
    }
}
