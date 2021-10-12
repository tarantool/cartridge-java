package io.tarantool.driver.api.connection;

import io.tarantool.driver.exceptions.NoAvailableConnectionsException;

/**
 * Implementations of this class contain an algorithm and maintain the necessary state for selecting
 * the next available connection from the supplied set of connections
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public interface ConnectionSelectionStrategy {
    /**
     * Provide the next available connection from the underlying pool of connections
     * @return an established connection
     * @throws NoAvailableConnectionsException if no connections exist or all connections are not usable
     */
    TarantoolConnection next() throws NoAvailableConnectionsException;
}
