package io.tarantool.driver.core;

import io.tarantool.driver.ConnectionSelectionStrategy;
import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolClientNotConnectedException;
import io.tarantool.driver.exceptions.TarantoolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean connectionMode = new AtomicBoolean(true);
    // resettable barrier for preventing multiple threads from running into the connection init sequence
    private final Phaser initPhaser = new Phaser(0);
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    public AbstractTarantoolConnectionManager(TarantoolClientConfig config,
                                              TarantoolConnectionFactory connectionFactory,
                                              ConnectionSelectionStrategyFactory selectStrategyFactory,
                                              TarantoolConnectionListeners connectionListeners) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.selectStrategyFactory = selectStrategyFactory;
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
    public TarantoolConnection getConnection() {
        try {
            TarantoolConnection connection = getConnectionInternal()
                    .get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
            if (!connection.isConnected()) {
                throw new TarantoolClientNotConnectedException();
            }
            return connection;
        } catch (InterruptedException | TimeoutException e) {
            throw new TarantoolClientException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TarantoolException) {
                throw (TarantoolException) e.getCause();
            }
            throw new TarantoolClientException(e);
        }
    }

    private CompletableFuture<TarantoolConnection> getConnectionInternal() {
        CompletableFuture<TarantoolConnection> result;
        if (initPhaser.getRegisteredParties() == 0 && connectionMode.compareAndSet(true, false)) {
            // Only one thread can reach to this line because of CAS. Rise up the barrier for 1 thread
            initPhaser.register();
            result = establishConnections()
                .thenAccept(registry -> {
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
                        connectionMode.set(true);
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
            if (aliveConnections.size() != config.getConnections()) {
                for (TarantoolConnection aliveConnection : aliveConnections) {
                    try {
                        aliveConnection.close();
                    } catch (Exception e) {
                        logger.error("Failed to close the connection", e);
                    }
                }
                CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>> connectionFuture =
                        establishConnectionsToEndpoint(serverAddress)
                            .thenApply(connections -> new AbstractMap.SimpleEntry<>(serverAddress, connections));
                endpointConnections.add(connectionFuture);
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
            TarantoolServerAddress serverAddress) {
        List<CompletableFuture<TarantoolConnection>> connections = connectionFactory
            .multiConnection(serverAddress.getSocketAddress(), config.getConnections()).stream()
            .peek(cf -> cf.thenApply(conn -> {
                if (conn.isConnected()) {
                    logger.info("Connected to Tarantool server at {}", conn.getRemoteAddress());
                }
                conn.addConnectionFailureListener(ex -> {
                    logger.error("Disconnected from Tarantool server", ex);
                    // Connection lost, signal the next thread coming for connection to start the init sequence
                    connectionMode.set(true);
                });
                return conn;
            }))
            .collect(Collectors.toList());
        return CompletableFuture
                .allOf(connections.toArray(new CompletableFuture[0]))
                .thenApply(v -> connections.parallelStream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()));
    }

    @Override
    public void close() {
        initPhaser.awaitAdvance(0);
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
