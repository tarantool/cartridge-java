package io.tarantool.driver.proxy;

import io.tarantool.driver.api.TarantoolResult;

import java.util.concurrent.CompletableFuture;

/**
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public interface ProxyOperation<T> {

    CompletableFuture<TarantoolResult<T>> execute();
}
