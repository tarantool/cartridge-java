package io.tarantool.driver.api.connection;

import io.tarantool.driver.api.TarantoolClientConfig;

import java.util.Collection;

/**
 * Manages instantiation of connection selection strategies. A strategy contains the algorithm of selecting connections
 * from the connection pool for performing the next request
 *
 * @author Alexey Kuzin
 * @see ConnectionSelectionStrategy
 */
public interface ConnectionSelectionStrategyFactory {
    /**
     * Take the specified collection of Tarantool server connections and instantiate a strategy
     *
     * @param config      client configuration
     * @param connections established connections
     * @return a connection selection strategy instance
     */
    ConnectionSelectionStrategy create(TarantoolClientConfig config, Collection<TarantoolConnection> connections);
}
