package io.tarantool.driver.core.connection;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategy;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Contains basic connection establishing and selection strategy invocation algorithms. Subclasses must implement
 * the retrieving of Tarantool server addresses.
 *
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolConnectionManager implements TarantoolConnectionManager {

    private final TarantoolClientConfig config;
    private final TarantoolConnectionFactory connectionFactory;
    private final ConnectionSelectionStrategyFactory selectStrategyFactory;
    private final TarantoolConnectionListeners connectionListeners;
    private Map<TarantoolServerAddress, List<TarantoolConnection>> connectionRegistry;
    private final AtomicReference<ConnectionSelectionStrategy> connectionSelectStrategy = new AtomicReference<>();
    // connection init sequence state
    private final AtomicReference<ConnectionMode> connectionMode = new AtomicReference<>(ConnectionMode.FULL);
    // resettable barrier for preventing multiple threads from running into the connection init sequence
    private final Phaser initPhaser = new Phaser(0);

    private static final Logger logger = LoggerFactory.getLogger(AbstractTarantoolConnectionManager.class);

    /**
     * Basic constructor
     *
     * @param config              Tarantool client config
     * @param connectionFactory   connection factory
     * @param connectionListeners connection listeners
     */
    public AbstractTarantoolConnectionManager(TarantoolClientConfig config,
                                              TarantoolConnectionFactory connectionFactory,
                                              TarantoolConnectionListeners connectionListeners) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.selectStrategyFactory = config.getConnectionSelectionStrategyFactory();
        this.connectionSelectStrategy.set(selectStrategyFactory.create(config, Collections.emptyList()));
        this.connectionListeners = connectionListeners;
        this.connectionRegistry = new HashMap<>();
    }

    /**
     * Get server addresses to connect to. They must belong to one cluster and contain the information necessary for
     * the internal {@link ConnectionSelectionStrategy} instance.
     *
     * @return Tarantool server addresses
     */
    protected abstract Collection<TarantoolServerAddress> getAddresses();

    @Override
    public CompletableFuture<TarantoolConnection> getConnection() {
        return getConnectionInternal().handle((connection, ex) -> {
            if (ex != null) {
                if (ex instanceof CompletionException) {
                    ex = ex.getCause();
                }
                if (ex instanceof NoAvailableConnectionsException) {
                    connectionMode.compareAndSet(ConnectionMode.OFF, ConnectionMode.FULL);
                }
                throw new TarantoolConnectionException(ex);
            }
            return connection;
        });
    }

    @Override
    public boolean refresh() {
        return connectionMode.compareAndSet(ConnectionMode.OFF, ConnectionMode.PARTIAL);
    }

    private CompletableFuture<TarantoolConnection> getConnectionInternal() {
        CompletableFuture<TarantoolConnection> result;
        ConnectionMode currentMode = connectionMode.get();
        if (initPhaser.getRegisteredParties() == 0 &&
                (connectionMode.compareAndSet(ConnectionMode.FULL, ConnectionMode.IN_PROGRESS) ||
                        connectionMode.compareAndSet(ConnectionMode.PARTIAL, ConnectionMode.IN_PROGRESS))) {
            AtomicReference<Map<TarantoolServerAddress, List<TarantoolConnection>>> currentRegistry =
                    new AtomicReference<>();
            logger.debug("Current connection mode: {}", currentMode);
            // Only one thread can reach to this line because of CAS. Rise up the barrier for 1 thread
            if (currentMode == ConnectionMode.FULL) {
                // We block the incoming requests until the connections are established and the registry is updated
                initPhaser.register();
            }

            result = establishConnections()
                    .thenAccept(registry -> {
                        if (currentMode == ConnectionMode.PARTIAL) {
                            // We block the incoming requests just before updating the registry
                            initPhaser.register();
                        }

                        currentRegistry.set(connectionRegistry);
                        // Add all alive connections
                        connectionRegistry = registry;
                        ConnectionSelectionStrategy strategy =
                                selectStrategyFactory.create(config, registry.values().stream()
                                        .flatMap(Collection::stream)
                                        .collect(Collectors.toList()));
                        connectionSelectStrategy.set(strategy);
                    })
                    .thenApply(v -> connectionSelectStrategy.get().next())
                    .whenComplete((v, ex) -> {
                        if (ex != null) {
                            // Connection attempt failed, signal the next thread coming for connection
                            // to start the init sequence
                            connectionMode.set(currentMode);
                        } else {
                            closeOldConnections(currentRegistry.get());
                        }
                        // Connection init sequence completed, release all waiting threads and lower the barrier
                        initPhaser.arriveAndDeregister();
                        connectionMode.compareAndSet(ConnectionMode.IN_PROGRESS, ConnectionMode.OFF);
                    });
        } else {
            // Wait until a thread finishes the init sequence and lowers the barrier. Once the barrier is lowered, the
            // phaser advances to the next phase.
            // As soon as all parties have arrived (register() is called once, so a single thread), all threads blocked
            // on awaitAdvance() continue running. getPhase() in this case returns the last phase value before
            // the termination.
            initPhaser.awaitAdvance(initPhaser.getPhase());
            // This call may produce NoAvailableConnectionsException if the connection attempt failed in all threads
            // that waited for the init sequence completion on the line above.
            // In this case the calling code may perform the request again.
            result = new CompletableFuture<>();
            try {
                result.complete(connectionSelectStrategy.get().next());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        }
        return result;
    }

    private CompletableFuture<Map<TarantoolServerAddress, List<TarantoolConnection>>> establishConnections()
            throws TarantoolClientException {
        CompletableFuture<Map<TarantoolServerAddress, List<TarantoolConnection>>> result = new CompletableFuture<>();
        try {
            List<CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>>> endpointConnections =
                    getConnections();
            result = CompletableFuture
                    .allOf(endpointConnections.toArray(new CompletableFuture[0]))
                    .thenApply(v -> endpointConnections.parallelStream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        } catch (Throwable e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    private List<CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>>> getConnections() {
        Collection<TarantoolServerAddress> addresses = getAddresses();
        if (addresses == null) {
            addresses = Collections.emptyList();
            logger.debug("The list of server addresses is not defined");
        }

        List<CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>>> endpointConnections =
                new ArrayList<>(addresses.size());
        for (TarantoolServerAddress serverAddress : addresses) {
            List<TarantoolConnection> aliveConnections = getAliveConnections(serverAddress);
            if (aliveConnections.size() < config.getConnections()) {
                CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>> connectionFuture =
                        establishConnectionsToEndpoint(serverAddress,
                                config.getConnections() - aliveConnections.size())
                                .thenApply(connections -> {
                                    connections.addAll(aliveConnections);
                                    return new AbstractMap.SimpleEntry<>(serverAddress, connections);
                                });
                endpointConnections.add(connectionFuture);
            } else if (aliveConnections.size() > config.getConnections()) {
                int count = config.getConnections() - aliveConnections.size();
                for (TarantoolConnection aliveConnection : aliveConnections) {
                    if (count-- > 0) {
                        try {
                            logger.info("Closing connection to {}, connections size is greater than {}",
                                    aliveConnection.getRemoteAddress(), config.getConnections());
                            aliveConnection.close();
                        } catch (Exception e) {
                            logger.info("Failed to close the connection: {}", e.getMessage());
                        }
                    } else {
                        break;
                    }
                }
            } else {
                endpointConnections.add(CompletableFuture.completedFuture(
                        new AbstractMap.SimpleEntry<>(serverAddress, aliveConnections)));
            }
        }
        return endpointConnections;
    }

    private List<TarantoolConnection> getAliveConnections(TarantoolServerAddress serverAddress) {
        List<TarantoolConnection> connections = connectionRegistry.getOrDefault(serverAddress, Collections.emptyList());
        return connections.stream().filter(TarantoolConnection::isConnected).collect(Collectors.toList());
    }

    private CompletableFuture<List<TarantoolConnection>> establishConnectionsToEndpoint(
            TarantoolServerAddress serverAddress, int connectionCount) {
        List<CompletableFuture<TarantoolConnection>> connections = connectionFactory
                .multiConnection(serverAddress.getSocketAddress(), connectionCount, connectionListeners).stream()
                .peek(cf -> cf.thenApply(conn -> {
                            if (conn.isConnected()) {
                                logger.info("Connected to Tarantool server at {}", conn.getRemoteAddress());
                            }
                            conn.addConnectionFailureListener((c, ex) -> {
                                // Connection lost, signal the next thread coming
                                // for connection to start the init sequence
                                connectionMode.set(ConnectionMode.PARTIAL);
                                try {
                                    c.close();
                                } catch (Exception e) {
                                    logger.info("Failed to close the connection: {}", e.getMessage());
                                }
                            });
                            conn.addConnectionCloseListener(
                                    c -> logger.info("Disconnected from {}", c.getRemoteAddress()));
                            return conn;
                        })
                )
                .collect(Collectors.toList());
        return CompletableFuture
                .allOf(connections.toArray(new CompletableFuture[0]))
                .thenApply(v -> connections.parallelStream()
                        .map(CompletableFuture::join)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()));
    }

    private void closeOldConnections(Map<TarantoolServerAddress, List<TarantoolConnection>> registry) {
        registry.forEach((key, value) -> {
            if (!connectionRegistry.containsKey(key)) {
                value.forEach(AbstractTarantoolConnectionManager::closeConnection);
            }
        });
    }

    @Override
    public void close() {
        if (initPhaser.getRegisteredParties() > 0) {
            initPhaser.awaitAdvance(initPhaser.getPhase());
        }
        connectionRegistry.values().stream()
                .flatMap(Collection::stream)
                .forEach(AbstractTarantoolConnectionManager::closeConnection);
    }

    private static void closeConnection(TarantoolConnection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            logger.warn("Failed to close connection: {}", e.getMessage());
        }
    }
}
