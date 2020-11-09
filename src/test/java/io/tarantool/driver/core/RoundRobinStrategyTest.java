package io.tarantool.driver.core;

import io.tarantool.driver.ConnectionSelectionStrategy;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RoundRobinStrategyTest {

    @Test
    public void testGetAddress() {
        List<TarantoolConnection> connections = Arrays.asList(
                new CustomConnection("127.0.0.1", 3001),
                new CustomConnection("127.0.0.2", 3002),
                new CustomConnection("127.0.0.3", 3003)
        );

        TarantoolClientConfig config = new TarantoolClientConfig();
        ConnectionSelectionStrategy strategy = RoundRobinStrategyFactory.INSTANCE.create(config, connections);

        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());
        assertEquals("127.0.0.2", ((CustomConnection) strategy.next()).getHost());
        assertEquals("127.0.0.3", ((CustomConnection) strategy.next()).getHost());
        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());
    }

    @Test
    public void testBoundaryCases() {
        List<TarantoolConnection> connections = new ArrayList<>();
        TarantoolClientConfig config = new TarantoolClientConfig();

        assertThrows(IllegalArgumentException.class,
                () -> RoundRobinStrategyFactory.INSTANCE.create(config, null));
        assertThrows(NoAvailableConnectionsException.class,
                () -> RoundRobinStrategyFactory.INSTANCE.create(config, connections).next());

        connections.add(new CustomConnection("127.0.0.1", 3001));
        ConnectionSelectionStrategy strategy = RoundRobinStrategyFactory.INSTANCE.create(config, connections);
        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());
        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());
        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());

        connections.clear();
        assertEquals("127.0.0.1", ((CustomConnection) strategy.next()).getHost());
    }

    @Test
    public void testParallelGetAddress() {
        List<TarantoolConnection> connections = IntStream.range(1, 11)
                .mapToObj(i -> new CustomConnection(String.format("127.0.0.%d", i), 3000 + i))
                .collect(Collectors.toList());

        TarantoolClientConfig config = new TarantoolClientConfig();
        ConnectionSelectionStrategy strategy = RoundRobinStrategyFactory.INSTANCE.create(config, connections);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<?>> futures = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                for (int j = 0; j < 10; j++) {
                    ((CustomConnection) strategy.next()).count();
                }
            }, executor));
        }
        futures.forEach(CompletableFuture::join);

        for (TarantoolConnection c : connections) {
            assertEquals(10, ((CustomConnection) c).getCount(), "Each connection must have been used 10 times");
        }
    }

    @Test
    public void testNoAvailableConnectionException() {
        List<TarantoolConnection> connections = IntStream.range(1, 11)
                .mapToObj(i -> new CustomConnection(String.format("127.0.0.%d", i), 3000 + i))
                .peek(c -> c.setConnected(false))
                .collect(Collectors.toList());

        TarantoolClientConfig config = new TarantoolClientConfig();
        ConnectionSelectionStrategy strategy = RoundRobinStrategyFactory.INSTANCE.create(config, connections);

        assertThrows(NoAvailableConnectionsException.class, strategy::next, "Exception must be thrown");
    }

    @Test
    public void testSkipConnections() {
        List<TarantoolConnection> connections = IntStream.range(1, 11)
                .mapToObj(i -> new CustomConnection(String.format("127.0.0.%d", i), 3000 + i))
                .peek(c -> {
                    if (c.getPort() < 3006) {
                        c.setConnected(false);
                    }
                })
                .collect(Collectors.toList());

        TarantoolClientConfig config = new TarantoolClientConfig();
        ConnectionSelectionStrategy strategy = RoundRobinStrategyFactory.INSTANCE.create(config, connections);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        List<CompletableFuture<?>> futures = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                for (int j = 0; j < 10; j++) {
                    assertDoesNotThrow(() -> ((CustomConnection) strategy.next()).count());
                }
            }, executor));
        }
        futures.forEach(CompletableFuture::join);

        for (TarantoolConnection c : connections) {
            if (((CustomConnection) c).getPort() < 3006) {
                assertEquals(0, ((CustomConnection) c).getCount(), "Closed connection must have been used 0 times");
            } else {
                assertEquals(20, ((CustomConnection) c).getCount(), "Active connection must have been used 20 times");
            }
        }
    }
}
