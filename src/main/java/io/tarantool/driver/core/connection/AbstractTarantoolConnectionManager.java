package io.tarantool.driver.core.connection;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategy;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolDaemonThreadFactory;
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
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
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
    private final Semaphore semaphore;
    private final ScheduledExecutorService reconnectService;
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    /**
     * Constructor
     *
     * @param config                   Tarantool client config
     * @param connectionFactory        connection factory
     * @param selectionStrategyFactory connection selection strategy factory
     * @param connectionListeners      connection listeners
     * @deprecated
     */
    protected AbstractTarantoolConnectionManager(TarantoolClientConfig config,
                                                 TarantoolConnectionFactory connectionFactory,
                                                 ConnectionSelectionStrategyFactory selectionStrategyFactory,
                                                 TarantoolConnectionListeners connectionListeners) {
        this(config, connectionFactory, connectionListeners);
    }

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
        this.semaphore = new Semaphore(1, true);
        //todo: move to reconnection policy
        this.reconnectService = Executors
                .newSingleThreadScheduledExecutor(new TarantoolDaemonThreadFactory("tarantool-reconnect"));
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
                    connectionMode.set(ConnectionMode.FULL);
                }
                throw new TarantoolConnectionException(ex);
            }
            return connection;
        });
    }

    public void connect() {
        establishConnections().thenAccept(registry -> {
            connectionRegistry.set(registry);
            ConnectionSelectionStrategy strategy =
                    selectStrategyFactory.create(config, registry.values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList()));
            connectionSelectStrategy.set(strategy);
        }).join();
    }

    private CompletableFuture<TarantoolConnection> getConnectionInternal() {
        //todo: add lazy init
        if (connectionRegistry.get().isEmpty()) {
            try {
                semaphore.acquire();
                connect();
                semaphore.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        CompletableFuture<TarantoolConnection> result;

        result = new CompletableFuture<>();
        try {
            result.complete(connectionSelectStrategy.get().next());
        } catch (Throwable t) {
            result.completeExceptionally(t);
        }

        return result;
    }

    private CompletableFuture<Map<TarantoolServerAddress, List<TarantoolConnection>>> establishConnections()
            throws TarantoolClientException {
        CompletableFuture<Map<TarantoolServerAddress, List<TarantoolConnection>>> result = new CompletableFuture<>();
        try {
            result = CompletableFuture.supplyAsync(() -> getConnections().parallelStream()
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
        List<TarantoolConnection> connections = connectionRegistry.get()
                .getOrDefault(serverAddress, Collections.emptyList());
        return connections.stream().filter(TarantoolConnection::isConnected).collect(Collectors.toList());
    }

    private CompletableFuture<List<TarantoolConnection>> establishConnectionsToEndpoint(
            TarantoolServerAddress serverAddress, int connectionCount) {
        return CompletableFuture.supplyAsync(() -> connectionFactory
                .multiConnection(serverAddress.getSocketAddress(), connectionCount, connectionListeners)
                .map(CompletableFuture::join)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    @Override
    public void close() {
        if (initPhaser.getRegisteredParties() > 0) {
            initPhaser.awaitAdvance(initPhaser.getPhase());
        }
        connectionRegistry.get().values().stream()
                .flatMap(Collection::stream)
                .forEach(conn -> {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        logger.warn("Failed to close connection: {}", e.getMessage());
                    }
                });
    }
}
