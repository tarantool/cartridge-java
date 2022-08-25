package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.connection.TarantoolConnectionManager;
import io.tarantool.driver.core.space.TarantoolTupleSpace;

import java.util.Collection;
import java.util.Collections;

/**
 * {@link ClusterTarantoolClient} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class ClusterTarantoolTupleClient
        extends ClusterTarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    /**
     * Create a client. Default guest credentials will be used. Connects to a Tarantool server on localhost using the
     * default port (3301)
     */
    public ClusterTarantoolTupleClient() {
        this(new SimpleTarantoolCredentials());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server on localhost using
     * the default port (3301)
     *
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public ClusterTarantoolTupleClient(TarantoolCredentials credentials) {
        this(credentials, Collections.singletonList(new TarantoolServerAddress()));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * host and port.
     *
     * @param credentials Tarantool user credentials holder
     * @param host        valid host name or IP address
     * @param port        valid port number
     * @see TarantoolCredentials
     */
    public ClusterTarantoolTupleClient(TarantoolCredentials credentials, String host, int port) {
        this(credentials, Collections.singletonList(new TarantoolServerAddress(host, port)));
    }

    /**
     * Create a client. Connects to a Tarantool server using the specified host and port.
     *
     * @param config client configuration
     * @param host   valid host name or IP address
     * @param port   valid port number
     * @see TarantoolCredentials
     */
    public ClusterTarantoolTupleClient(TarantoolClientConfig config, String host, int port) {
        this(config, Collections.singletonList(new TarantoolServerAddress(host, port)));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * server address.
     *
     * @param credentials Tarantool user credentials holder
     * @param address     single Tarantool server address
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public ClusterTarantoolTupleClient(TarantoolCredentials credentials, TarantoolServerAddress address) {
        this(TarantoolClientConfig.builder()
                        .withCredentials(credentials)
                        .build(),
                address);
    }

    /**
     * Create a client. Connects to a Tarantool server using the specified
     * server address.
     *
     * @param config  client configuration
     * @param address single Tarantool server address
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public ClusterTarantoolTupleClient(TarantoolClientConfig config, TarantoolServerAddress address) {
        this(config, () -> Collections.singletonList(address));
    }

    /**
     * Create a client using provided credentials information. Connects to a list of Tarantool servers using the
     * specified set of server addresses.
     *
     * @param credentials Tarantool user credentials holder
     * @param addresses   Tarantool server addresses
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public ClusterTarantoolTupleClient(TarantoolCredentials credentials, Collection<TarantoolServerAddress> addresses) {
        this(TarantoolClientConfig.builder()
                        .withCredentials(credentials)
                        .withConnectionSelectionStrategyFactory(ParallelRoundRobinStrategyFactory.INSTANCE)
                        .build(),
                () -> addresses);
    }

    /**
     * Create a client. Connects to a list of Tarantool servers using the specified set of server addresses.
     *
     * @param config    client configuration
     * @param addresses Tarantool server addresses
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public ClusterTarantoolTupleClient(TarantoolClientConfig config, Collection<TarantoolServerAddress> addresses) {
        this(config, () -> addresses);
    }

    /**
     * Create a client. Connect using the list of Tarantool servers returned by the specified server address provider.
     *
     * @param config          client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @see TarantoolClientConfig
     */
    public ClusterTarantoolTupleClient(TarantoolClientConfig config, TarantoolClusterAddressProvider addressProvider) {
        super(config, addressProvider);
    }

    @Override
    protected TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    spaceOperations(TarantoolClientConfig config, TarantoolConnectionManager connectionManager,
                    TarantoolMetadataOperations metadata, TarantoolSpaceMetadata spaceMetadata) {
        return new TarantoolTupleSpace(this, config, connectionManager, metadata, spaceMetadata);
    }

    @Override
    protected TarantoolClusterAddressProvider getAddressProvider() {
        return super.getAddressProvider();
    }
}
