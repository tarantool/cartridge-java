package io.tarantool.driver.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.mappers.CallResultMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Basic implementation of a proxy operation
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {

    protected final TarantoolClient client;
    protected final String functionName;
    protected final List<?> arguments;
    protected final
    CallResultMapper<T, SingleValueCallResult<T>> resultMapper;

    AbstractProxyOperation(
            TarantoolClient client,
            String functionName,
            List<?> arguments,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        this.client = client;
        this.arguments = arguments;
        this.functionName = functionName;
        this.resultMapper = resultMapper;
    }

    public TarantoolClient getClient() {
        return client;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<?> getArguments() {
        return arguments;
    }

    public CallResultMapper<T, SingleValueCallResult<T>> getResultMapper() {
        return resultMapper;
    }

    @Override
    public CompletableFuture<T> execute() {
        return client.callForSingleResult(
                functionName, arguments, client.getConfig().getMessagePackMapper(), resultMapper);
    }
}
