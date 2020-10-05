package io.tarantool.driver.proxy;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {

    protected final TarantoolClient client;
    protected final String functionName;
    protected final List<Object> arguments;
    protected final TarantoolCallResultMapper<T> resultMapper;

    AbstractProxyOperation(TarantoolClient client,
                           String functionName,
                           List<Object> arguments,
                           TarantoolCallResultMapper<T> resultMapper) {
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

    public List<Object> getArguments() {
        return arguments;
    }

    public TarantoolCallResultMapper<T> getResultMapper() {
        return resultMapper;
    }

    @Override
    public CompletableFuture<TarantoolResult<T>> execute() {
        return client.call(functionName, arguments, client.getConfig().getMessagePackMapper(), resultMapper);
    }
}
