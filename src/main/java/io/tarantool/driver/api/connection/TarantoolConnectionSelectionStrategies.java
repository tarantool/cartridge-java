package io.tarantool.driver.api.connection;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.core.connection.TarantoolConnectionIterator;
import io.tarantool.driver.exceptions.NoAvailableConnectionsException;
import io.tarantool.driver.utils.Assert;
import io.tarantool.driver.utils.CyclingIterator;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Class-container for default kinds of connection selection strategies
 *
 * @author Alexey Kuzin
 */
public final class TarantoolConnectionSelectionStrategies {

    /**
     * Instantiates a {@link RoundRobinStrategy}, which is applicable for multiple connections to
     * one server and selects connections in the order according to the passed collection.
     */
    public enum RoundRobinStrategyFactory implements ConnectionSelectionStrategyFactory {
        INSTANCE;

        @Override
        public ConnectionSelectionStrategy create(
            TarantoolClientConfig config,
            Collection<TarantoolConnection> connections) {
            Assert.notNull(connections, "The collection of Tarantool connections should not be null");

            return new RoundRobinStrategy(connections);
        }
    }

    static final class RoundRobinStrategy implements ConnectionSelectionStrategy {

        private final TarantoolConnectionIterator connectionIterator;
        private final AtomicInteger available;
        private final int connectionsCount;

        RoundRobinStrategy(Collection<TarantoolConnection> connections) {
            this.connectionsCount = connections.size();
            this.available = new AtomicInteger(this.connectionsCount);
            this.connectionIterator = new TarantoolConnectionIterator(connections.stream()
                .peek(conn -> conn.addConnectionCloseListener(c -> available.getAndDecrement()))
                .collect(Collectors.toList()));
        }

        @Override
        public TarantoolConnection next() throws NoAvailableConnectionsException {
            int attempts = 0;
            while (available.get() > 0 && connectionIterator.hasNext()) {
                TarantoolConnection connection = connectionIterator.next();
                if (connection.isConnected()) {
                    return connection;
                }

                if (++attempts > connectionsCount) {
                    break;
                }
            }
            throw new NoAvailableConnectionsException();
        }
    }

    /**
     * Instantiates a {@link ParallelRoundRobinStrategy}, which is applicable for multiple
     * connections to several servers and expects equal number of connections per server. The connections are split into
     * parts with equal amounts and selected in the order according to the passed collection, with switching between
     * parts in the same order
     */
    public enum ParallelRoundRobinStrategyFactory implements ConnectionSelectionStrategyFactory {
        INSTANCE;

        @Override
        public ConnectionSelectionStrategy create(
            TarantoolClientConfig config,
            Collection<TarantoolConnection> connections) {
            Assert.notNull(connections, "The collection of Tarantool connections should not be null");

            return new ParallelRoundRobinStrategy(config, connections);
        }
    }

    static final class ParallelRoundRobinStrategy implements ConnectionSelectionStrategy {

        private final TarantoolClientConfig config;
        private final CyclingIterator<TarantoolConnectionIterator> iteratorsIterator;
        private final AtomicInteger available;
        private final int connectionsCount;

        ParallelRoundRobinStrategy(TarantoolClientConfig config, Collection<TarantoolConnection> connections) {
            this.config = config;
            this.connectionsCount = connections.size();
            this.available = new AtomicInteger(this.connectionsCount);
            this.iteratorsIterator = new CyclingIterator<>(populateIterators(connections));
        }

        private Collection<TarantoolConnectionIterator> populateIterators(
            Collection<TarantoolConnection> connections) {
            int groupSize = config.getConnections();
            AtomicInteger currentSize = new AtomicInteger(0);
            return connections.stream()
                .peek(conn -> conn.addConnectionCloseListener(c -> available.getAndDecrement()))
                .collect(Collectors.groupingBy(
                    conn -> currentSize.getAndIncrement() / groupSize)).values().stream()
                .map(TarantoolConnectionIterator::new)
                .filter(TarantoolConnectionIterator::hasNext)
                .collect(Collectors.toList());
        }

        @Override
        public TarantoolConnection next() throws NoAvailableConnectionsException {
            int attempts = 0;
            while (available.get() > 0 && iteratorsIterator.hasNext()) {
                TarantoolConnection connection = iteratorsIterator.next().next();
                if (connection.isConnected()) {
                    return connection;
                }
                if (++attempts > connectionsCount) {
                    break;
                }
            }
            throw new NoAvailableConnectionsException();
        }
    }
}
