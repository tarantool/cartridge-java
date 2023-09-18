package io.tarantool.driver.core.proxy.interfaces;

import java.util.concurrent.CompletableFuture;

/**
 * Base interface for space operations mapped to call operations
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public interface ProxyOperation<T> {
    /**
     * Perform operation.
     *
     * @return a future with operation result
     */
    CompletableFuture<T> execute();
}
