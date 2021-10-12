package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.core.connection.TarantoolClusterConnectionManager;
import io.tarantool.driver.core.connection.TarantoolConnectionFactory;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.core.connection.TarantoolConnectionManager;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.utils.Assert;

import java.util.Collection;

/**
 * Main class for connecting to a cluster of Tarantool servers. Provides basic API for interacting with the database
 * and manages connections. Connects to all configured Tarantool server addresses simultaneously and dynamically selects
 * the target server for performing the next request accordingly to the provided connection selection strategy. The
 * credentials for connecting to each server are expected to be the same.
 *
 * @param <T> target tuple type
 * @param <R> target tuple collection type
 * @author Alexey Kuzin
 */
public abstract class ClusterTarantoolClient<T extends Packable, R extends Collection<T>>
        extends AbstractTarantoolClient<T, R> {

    private final TarantoolClusterAddressProvider addressProvider;

    /**
     * Create a client. The server address for connecting to the server is specified by the passed address provider.
     *
     * @param config          the client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @see TarantoolClientConfig
     */
    public ClusterTarantoolClient(TarantoolClientConfig config,
                                  TarantoolClusterAddressProvider addressProvider) {
        super(config);

        Assert.notNull(addressProvider, "Address provider must not be null");

        this.addressProvider = addressProvider;
    }

    @Override
    protected TarantoolConnectionManager connectionManager(TarantoolClientConfig config,
                                                           TarantoolConnectionFactory connectionFactory,
                                                           TarantoolConnectionListeners listeners) {
        return new TarantoolClusterConnectionManager(config, connectionFactory, listeners, addressProvider);
    }

    protected TarantoolClusterAddressProvider getAddressProvider() {
        return addressProvider;
    }
}
