package io.tarantool.driver.api.connection;

import java.util.concurrent.CompletableFuture;

/**
 * Listens on the connection future and asynchronously performs some operation over the connection
 * once it is ready
 *
 * @author Alexey Kuzin
 */
public interface TarantoolConnectionListener {
    /**
     * The operation to perform when the connection is ready
     *
     * @param connection established connection to the Tarantool server
     * @return operation result future
     */
    CompletableFuture<TarantoolConnection> onConnection(TarantoolConnection connection);
}
