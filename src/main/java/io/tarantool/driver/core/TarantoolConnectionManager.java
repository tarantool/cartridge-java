package io.tarantool.driver.core;

import io.tarantool.driver.api.connection.TarantoolConnection;

import java.util.concurrent.CompletableFuture;

/**
 * Manages the Tarantool server connections lifecycle. Maintains multiple connections. Once a connection is lost,
 * a connection procedure is performed.
 *
 * @author Alexey Kuzin
 */
public interface TarantoolConnectionManager extends AutoCloseable {
    /**
     * Get an established connection according to the order provided by specified connection selection strategy. If the
     * connection procedure hasn't been performed yet, starts it.
     *
     * @return a future with next connection in order
     */
    CompletableFuture<TarantoolConnection> getConnection();
}
