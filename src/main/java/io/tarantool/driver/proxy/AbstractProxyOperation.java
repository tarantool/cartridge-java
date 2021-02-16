package io.tarantool.driver.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Basic implementation of a proxy operation
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {

    protected final TarantoolCallOperations client;
    protected final String functionName;
    protected final List<?> arguments;
    private final MessagePackObjectMapper argumentsMapper;
    protected final CallResultMapper<T, SingleValueCallResult<T>> resultMapper;

    AbstractProxyOperation(
            TarantoolCallOperations client,
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        this.client = client;
        this.argumentsMapper = argumentsMapper;
        this.arguments = arguments;
        this.functionName = functionName;
        this.resultMapper = resultMapper;
    }

    public TarantoolCallOperations getClient() {
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
        return client.callForSingleResult(functionName, arguments, argumentsMapper, resultMapper);
    }
}
