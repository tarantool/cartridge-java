package io.tarantool.driver;

import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolClusterConnectionManager;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * Main class for connecting to a cluster of Tarantool servers. Provides basic API for interacting with the database
 * and manages connections. Connects to all configured Tarantool server addresses simultaneously.  The credentials
 * for connecting to each server are expected to be the same.
 *
 * @author Alexey Kuzin
 */
public class ClusterTarantoolClient extends AbstractTarantoolClient {

    private final ConnectionSelectionStrategyFactory selectStrategyFactory;
    private TarantoolClusterAddressProvider addressProvider;

    /**
     * Create a client. Default guest credentials will be used. Connects to a Tarantool server on localhost using the
     * default port (3301)
     */
    public ClusterTarantoolClient() {
        this(new SimpleTarantoolCredentials());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server on localhost using
     * the default port (3301)
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public ClusterTarantoolClient(TarantoolCredentials credentials) {
       this(credentials, Collections.singletonList(new TarantoolServerAddress()));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * host and port.
     * @param credentials Tarantool user credentials holder
     * @param host valid host name or IP address
     * @param port valid port number
     * @see TarantoolCredentials
     */
    public ClusterTarantoolClient(TarantoolCredentials credentials, String host, int port) {
       this(credentials, Collections.singletonList(new TarantoolServerAddress(host, port)));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * server address.
     * @param credentials Tarantool user credentials holder
     * @param addresses Tarantool server addresses
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public ClusterTarantoolClient(TarantoolCredentials credentials, Collection<TarantoolServerAddress> addresses) {
       this(TarantoolClientConfig.builder()
               .withCredentials(credentials)
               .build(),
           ParallelRoundRobinStrategyFactory.INSTANCE,
           () -> addresses);
    }

    /**
     * Create a client. The server address for connecting to the server is specified by the passed address provider.
     * @param config the client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @see TarantoolClientConfig
     */
    public ClusterTarantoolClient(TarantoolClientConfig config,
                                  ConnectionSelectionStrategyFactory selectStrategyFactory,
                                  TarantoolClusterAddressProvider addressProvider) {
        super(config);
        this.selectStrategyFactory = selectStrategyFactory;
        this.addressProvider = addressProvider;
    }

    @Override
    protected TarantoolConnectionManager connectionManager(TarantoolClientConfig config,
                                                           TarantoolConnectionFactory connectionFactory,
                                                           TarantoolConnectionListeners listeners) {
        return new TarantoolClusterConnectionManager(
                config, connectionFactory, selectStrategyFactory, listeners, addressProvider);
    }
}
