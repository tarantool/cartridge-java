package io.tarantool.driver.core;

import io.tarantool.driver.ConnectionSelectionStrategy;
import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import io.tarantool.driver.exceptions.TarantoolClientException;
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
    private final AtomicReference<Map<TarantoolServerAddress, List<TarantoolConnection>>> connectionRegistry =
            new AtomicReference<>(new HashMap<>());
    private final AtomicReference<ConnectionSelectionStrategy> connectionSelectStrategy = new AtomicReference<>();
    // connection init sequence state
    private final AtomicReference<ConnectionMode> connectionMode = new AtomicReference<>(ConnectionMode.FULL);
    // resettable barrier for preventing multiple threads from running into the connection init sequence
    private final Phaser initPhaser = new Phaser(0);
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    /**
     * Constructor
     * @deprecated
     * @param config Tarantool client config
     * @param connectionFactory connection factory
     * @param selectionStrategyFactory connection selection strategy factory
     * @param connectionListeners connection listeners
     */
    protected AbstractTarantoolConnectionManager(TarantoolClientConfig config,
                                                 TarantoolConnectionFactory connectionFactory,
                                                 ConnectionSelectionStrategyFactory selectionStrategyFactory,
                                                 TarantoolConnectionListeners connectionListeners) {
        this(config, connectionFactory, connectionListeners);
    }

    /**
     * Basic constructor
     * @param config Tarantool client config
     * @param connectionFactory connection factory
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
    }

    /**
     * Get server addresses to connect to. They must belong to one cluster and contain the information necessary for
     * the internal {@link ConnectionSelectionStrategy} instance.
     * @return Tarantool server addresses
     */
    protected abstract Collection<TarantoolServerAddress> getAddresses();

    @Override
    public CompletableFuture<TarantoolConnection> getConnection() {
        return getConnectionInternal().handle((connection, ex) -> {
            if (ex != null) {
                if (ex instanceof NoAvailableConnectionsException) {
                    connectionMode.set(ConnectionMode.FULL);
                }
                throw new CompletionException(ex);
            }
            return connection;
        });
    }

    private CompletableFuture<TarantoolConnection> getConnectionInternal() {
        CompletableFuture<TarantoolConnection> result;
        ConnectionMode currentMode = connectionMode.get();
        if (initPhaser.getRegisteredParties() == 0 &&
                (connectionMode.compareAndSet(ConnectionMode.FULL, ConnectionMode.OFF) ||
                        connectionMode.compareAndSet(ConnectionMode.PARTIAL, ConnectionMode.OFF))) {
            // Only one thread can reach to this line because of CAS. Rise up the barrier for 1 thread
            if (currentMode == ConnectionMode.FULL) {
                initPhaser.register();
            }
            result = establishConnections()
                .thenAccept(registry -> {
                    if (currentMode == ConnectionMode.PARTIAL) {
                        initPhaser.register();
                    }
                    // Add all alive connections
                    connectionRegistry.set(registry);
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
                    }
                    // Connection init sequence completed, release all waiting threads and lower the barrier
                    initPhaser.arriveAndDeregister();
                });
            for (TarantoolConnectionListener connectionListener : connectionListeners.all()) {
                result = result.thenCompose(connectionListener::onConnection);
            }
        } else {
            // Wait until a thread finishes the init sequence and lowers the barrier. Once the barrier is lowered, the
            // phaser advances to the next phase.
            // As soon as all parties have arrived (register() is called once, so a single thread), all threads blocked
            // on awaitAdvance() continue running. getPhase() in thiss case returns the last phase value before
            // the termination.
            initPhaser.awaitAdvance(initPhaser.getPhase());
            // This call may produce NoAvailableConnectionsException if the connection attempt failed in all threads
            // that waited for the init sequence completion on the line above.
            // In this case the calling code may perform the request again.
            result = CompletableFuture.completedFuture(connectionSelectStrategy.get().next());
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
        List<CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>>> endpointConnections =
                new ArrayList<>(addresses.size());
        for (TarantoolServerAddress serverAddress : addresses) {
            List<TarantoolConnection> aliveConnections = getAliveConnections(serverAddress);
            if (aliveConnections.size() < config.getConnections()) {
                CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>> connectionFuture =
                        establishConnectionsToEndpoint(serverAddress, config.getConnections() - aliveConnections.size())
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
                            aliveConnection.close();
                        } catch (Exception e) {
                            logger.error("Failed to close the connection", e);
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
        List<TarantoolConnection> connections = connectionRegistry.get()
                .getOrDefault(serverAddress, Collections.emptyList());
        return connections.stream().filter(TarantoolConnection::isConnected).collect(Collectors.toList());
    }

    private CompletableFuture<List<TarantoolConnection>> establishConnectionsToEndpoint(
            TarantoolServerAddress serverAddress, int connectionCount) {
        List<CompletableFuture<TarantoolConnection>> connections = connectionFactory
            .multiConnection(serverAddress.getSocketAddress(), connectionCount).stream()
            .peek(cf -> cf.thenApply(conn -> {
                    if (conn.isConnected()) {
                        logger.info("Connected to Tarantool server at {}", conn.getRemoteAddress());
                    }
                    conn.addConnectionFailureListener((c, ex) -> {
                        logger.error("Disconnected from {}", c.getRemoteAddress(), ex);
                        // Connection lost, signal the next thread coming for connection to start the init sequence
                        connectionMode.set(ConnectionMode.PARTIAL);
                    });
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

    @Override
    public void close() {
        initPhaser.awaitAdvance(initPhaser.getPhase());
        connectionRegistry.get().values().stream()
            .flatMap(Collection::stream)
            .forEach(conn -> {
                try {
                    conn.close();
                } catch (Exception e) {
                    logger.error("Failed to close connection", e);
                }
            });
    }
}
