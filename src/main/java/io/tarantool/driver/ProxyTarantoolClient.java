package io.tarantool.driver;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolTupleFactory;
import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.proxy.ProxyOperationsMapping;
import io.tarantool.driver.metadata.ProxyTarantoolMetadata;
import org.msgpack.value.ArrayValue;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client implementation that decorates a {@link TarantoolClient} instance, proxying all CRUD operations through the
 * instance's <code>call</code> method to the proxy functions defined on the Tarantool instance(s).
 *
 * Proxy functions to be called can be specified by overriding the methods of the implemented
 * {@link ProxyOperationsMapping} interface. These functions must be public API functions available on the Tarantool
 * instance fro the connected API user.
 *
 * It is recommended to use this client with the CRUD module (<a href="https://github.com/tarantool/crud">
 * https://github.com/tarantool/crud</a>) installed on the target Tarantool instance.
 *
 * See <a href="https://github.com/tarantool/examples/blob/master/profile-storage/README.md">
 *     https://github.com/tarantool/examples/blob/master/profile-storage/README.md</a>
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public class ProxyTarantoolClient implements TarantoolClient, ProxyOperationsMapping {

    private final TarantoolClient client;
    private final AtomicReference<ProxyTarantoolMetadata> metadataHolder = new AtomicReference<>();

    public ProxyTarantoolClient(TarantoolClient decoratedClient) {
        this.client = decoratedClient;
        this.client.getListeners().clear();
        this.client.getListeners().add(connection -> {
            try {
                return metadata().refresh().thenApply(v -> connection);
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        });
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceById(spaceId);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceId);
        }

        return new ProxyTarantoolSpace(this, meta.get());
    }

    @Override
    public TarantoolClientConfig getConfig() {
        return client.getConfig();
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        return client.getVersion();
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }

        return new ProxyTarantoolSpace(this, meta.get());
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        if (metadataHolder.get() == null) {
            this.metadataHolder.compareAndSet(
                    null, new ProxyTarantoolMetadata(this.getGetSchemaFunctionName(), this));
        }
        return metadataHolder.get();
    }

    @Override
    public TarantoolConnectionListeners getListeners() {
        return this.client.getListeners();
    }

    @Override
    public TarantoolTupleFactory getTarantoolTupleFactory() {
        return this.client.getTarantoolTupleFactory();
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
    public CompletableFuture<List<Object>> call(String functionName, List<Object> arguments, MessagePackMapper mapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, mapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, Class<T> entityClass)
            throws TarantoolClientException {
        return client.call(functionName, entityClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return client.call(functionName, tupleMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return client.call(functionName, arguments, entityClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, tupleMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return client.call(functionName, arguments, argumentsMapper, entityClass);
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
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          TarantoolCallResultMapper<T> resultMapper)
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
    public CompletableFuture<List<Object>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, resultMapper);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression,
                                                List<Object> arguments,
                                                MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return client.eval(expression, arguments, resultMapper);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression,
                                                List<Object> arguments,
                                                MessagePackObjectMapper argumentsMapper,
                                                MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return client.eval(expression, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
