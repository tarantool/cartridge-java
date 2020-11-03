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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
    private final AtomicBoolean connectionMode = new AtomicBoolean(true);
    private final CountDownLatch initLatch = new CountDownLatch(1);
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
        if (connectionMode.compareAndSet(true, false)) {
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
                    if (initLatch.getCount() > 0) {
                        initLatch.countDown();
                    }
                });
            for (TarantoolConnectionListener connectionListener : connectionListeners.all()) {
                result = result.thenCompose(connectionListener::onConnection);
            }
        } else {
            try {
                initLatch.await();
            } catch (InterruptedException e) {
                throw new TarantoolClientException("Interrupted while waiting for connection manager initialization");
            }
            result = CompletableFuture.completedFuture(connectionSelectStrategy.get().next());
        }
        return result;
    }

    private CompletableFuture<Map<TarantoolServerAddress, List<TarantoolConnection>>> establishConnections()
            throws TarantoolClientException {
        List<CompletableFuture<Map.Entry<TarantoolServerAddress, List<TarantoolConnection>>>> endpointConnections =
                getConnections();
        return CompletableFuture
                .allOf(endpointConnections.toArray(new CompletableFuture[0]))
                .thenApply(v -> endpointConnections.parallelStream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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
                conn.addConnectionFailureListener(ex -> {
                    logger.error("Disconnected from Tarantool server", ex);
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
        try {
            initLatch.await();
        } catch (InterruptedException e) {
            throw new TarantoolClientException("Interrupted while waiting for connection manager initialization");
        }
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
