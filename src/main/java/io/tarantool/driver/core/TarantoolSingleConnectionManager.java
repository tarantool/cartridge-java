package io.tarantool.driver.core;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.TarantoolSingleAddressProvider;

import java.util.Collection;
import java.util.Collections;

/**
 * Implementation of {@link AbstractTarantoolConnectionManager}, aware of connecting to a single Tarantool server
 *
 * @author Alexey Kuzin
 */
public class TarantoolSingleConnectionManager extends AbstractTarantoolConnectionManager {

    private final TarantoolSingleAddressProvider addressProvider;

    /**
     * Basic constructor.
     *
     * @param config client configuration
     * @param connectionFactory manages instantiation of Tarantool server connections
     * @param selectStrategyFactory manages selection of the next connection from available ones
     * @param listeners are invoked after connection is established
     * @param addressProvider provides Tarantool server node address
     */
    public TarantoolSingleConnectionManager(TarantoolClientConfig config,
                                            TarantoolConnectionFactory connectionFactory,
                                            ConnectionSelectionStrategyFactory selectStrategyFactory,
                                            TarantoolConnectionListeners listeners,
                                            TarantoolSingleAddressProvider addressProvider) {
        super(config, connectionFactory, selectStrategyFactory, listeners);
        this.addressProvider = addressProvider;
    }

    @Override
    protected Collection<TarantoolServerAddress> getAddresses() {
        return Collections.singleton(addressProvider.getAddress());
    }
}
