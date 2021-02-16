package io.tarantool.driver;

import io.tarantool.driver.core.TarantoolClusterConnectionManager;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
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

    private final ConnectionSelectionStrategyFactory selectStrategyFactory;
    private final TarantoolClusterAddressProvider addressProvider;

    /**
     * Create a client. The server address for connecting to the server is specified by the passed address provider.
     * @param config the client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @param selectStrategyFactory instantiates strategies which provide the algorithm of selecting connections
     *                              from the connection pool for performing the next request
     * @see TarantoolClientConfig
     */
    public ClusterTarantoolClient(TarantoolClientConfig config,
                                  TarantoolClusterAddressProvider addressProvider,
                                  ConnectionSelectionStrategyFactory selectStrategyFactory) {
        super(config);

        Assert.notNull(addressProvider, "Address provider must not be null");
        Assert.notNull(selectStrategyFactory, "Connection selection strategy factory must not be null");

        this.addressProvider = addressProvider;
        this.selectStrategyFactory = selectStrategyFactory;
    }

    @Override
    protected TarantoolConnectionManager connectionManager(TarantoolClientConfig config,
                                                           TarantoolConnectionFactory connectionFactory,
                                                           TarantoolConnectionListeners listeners) {
        return new TarantoolClusterConnectionManager(
                config, connectionFactory, selectStrategyFactory, listeners, addressProvider);
    }
}
