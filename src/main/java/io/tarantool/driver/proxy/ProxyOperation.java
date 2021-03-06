package io.tarantool.driver.proxy;

import java.util.concurrent.CompletableFuture;

/**
 * Base interface for space operations mapped to call operations
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public interface ProxyOperation<T> {

    CompletableFuture<T> execute();
}
