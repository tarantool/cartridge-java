package io.tarantool.driver;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tarantool client decorator
 *
 * @author Sergey Volgin
 */
abstract class ProxyTarantoolClient implements TarantoolClient {

    protected TarantoolClient client;

    @Override
    public TarantoolClientConfig getConfig() {
        return client.getConfig();
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        return client.getVersion();
    }

    @Override
    public TarantoolConnectionListeners getListeners() {
        return client.getListeners();
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName) throws TarantoolClientException {
        return client.call(functionName);
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName, List<Object> arguments)
            throws TarantoolClientException {
        return client.call(functionName, arguments);
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName,
                                               List<Object> arguments,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, resultMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, argumentsMapper, tupleMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName,
                                               List<Object> arguments,
                                               MessagePackObjectMapper argumentsMapper,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression) throws TarantoolClientException {
        return client.eval(expression);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression, List<Object> arguments)
            throws TarantoolClientException {
        return client.eval(expression, arguments);
    }

    @Override
    public <T> CompletableFuture<List<T>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> eval(String expression,
                                               List<Object> arguments,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, arguments, resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> eval(String expression,
                                               List<Object> arguments,
                                               MessagePackObjectMapper argumentsMapper,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
