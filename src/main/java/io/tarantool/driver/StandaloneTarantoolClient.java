package io.tarantool.driver;

import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;
import io.tarantool.driver.core.TarantoolSingleConnectionManager;

/**
 * Main class for connecting to a single Tarantool server. Provides basic API for interacting with the database
 * and manages connections.
 *
 * @author Alexey Kuzin
 */
public class StandaloneTarantoolClient extends AbstractTarantoolClient {

    private final ConnectionSelectionStrategyFactory selectStrategyFactory;
    private final TarantoolSingleAddressProvider addressProvider;

    /**
     * Create a client. Default guest credentials will be used. Connects to a Tarantool server on localhost using the
     * default port (3301)
     */
    public StandaloneTarantoolClient() {
        this(new SimpleTarantoolCredentials());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server on localhost using
     * the default port (3301)
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials) {
       this(credentials, new TarantoolServerAddress());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * host and port.
     * @param credentials Tarantool user credentials holder
     * @param host valid host name or IP address
     * @param port valid port number
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials, String host, int port) {
       this(credentials, new TarantoolServerAddress(host, port));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * server address.
     * @param credentials Tarantool user credentials holder
     * @param address Tarantool server address
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials, TarantoolServerAddress address) {
       this(TarantoolClientConfig.builder()
               .withCredentials(credentials)
               .build(),
           address);
    }

    /**
     * Create a client. Connects to a Tarantool server using the specified server address. The default connection
     * selection strategy is used.
     * @param config client configuration
     * @param address Tarantool server address
     * @see TarantoolCredentials
     * @see RoundRobinStrategyFactory
     * @see TarantoolServerAddress
     */
    public StandaloneTarantoolClient(TarantoolClientConfig config, TarantoolServerAddress address) {
       this(config, () -> address, RoundRobinStrategyFactory.INSTANCE);
    }

    /**
     * Create a client. The server address for connecting to the server is specified by the passed address provider.
     * @param config client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @param selectStrategyFactory instantiates strategies which provide the algorithm of selecting connections
     *                              from the connection pool for performing the next request
     * @see TarantoolClientConfig
     */
    public StandaloneTarantoolClient(TarantoolClientConfig config,
                                     TarantoolSingleAddressProvider addressProvider,
                                     ConnectionSelectionStrategyFactory selectStrategyFactory) {
        super(config);
        this.addressProvider = addressProvider;
        this.selectStrategyFactory = selectStrategyFactory;
    }

    @Override
    protected TarantoolConnectionManager connectionManager(TarantoolClientConfig config,
                                                           TarantoolConnectionFactory connectionFactory,
                                                           TarantoolConnectionListeners listeners) {
        return new TarantoolSingleConnectionManager(
                config, connectionFactory, selectStrategyFactory, listeners, addressProvider);
    }
}
