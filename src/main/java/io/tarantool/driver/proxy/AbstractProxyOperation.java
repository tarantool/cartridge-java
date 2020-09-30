package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {
    protected final TarantoolClient client;
    protected final String functionName;
    protected List<Object> arguments;
    protected final ValueConverter<ArrayValue, T> tupleMapper;

    AbstractProxyOperation(TarantoolClient client,
                           String functionName,
                           List<Object> arguments,
                           ValueConverter<ArrayValue, T> tupleMapper) {
        this.client = client;
        this.arguments = arguments;
        this.functionName = functionName;
        this.tupleMapper = tupleMapper;
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

    public ValueConverter<ArrayValue, T> getTupleMapper() {
        return tupleMapper;
    }

    @Override
    public CompletableFuture<TarantoolResult<T>> execute() {
        return client.call(functionName, arguments, client.getConfig().getMessagePackMapper(), tupleMapper);
    }
}
