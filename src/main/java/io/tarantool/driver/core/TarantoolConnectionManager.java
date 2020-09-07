package io.tarantool.driver.core;

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
     * @return next connection in order
     */
    CompletableFuture<TarantoolConnection> getConnection();
}
