package io.tarantool.driver;

import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
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
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.ResultMapperFactoryFactory;
import io.tarantool.driver.metadata.DDLTarantoolSpaceMetadataConverter;
import io.tarantool.driver.metadata.ProxyMetadataProvider;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.proxy.ProxyOperationsMapping;
import io.tarantool.driver.utils.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Client implementation that decorates a {@link TarantoolClient} instance, proxying all CRUD operations through the
 * instance's <code>call</code> method to the proxy functions defined on the Tarantool instance(s).
 *
 * Proxy functions to be called can be specified by overriding the methods of the implemented
 * {@link ProxyOperationsMapping} interface. These functions must be public API functions available on the Tarantool
 * instance for the connected API user.
 *
 * It is recommended to use this client with the CRUD module (<a href="https://github.com/tarantool/crud">
 * https://github.com/tarantool/crud</a>) installed on the target Tarantool instance. Be sure that the server instances
 * you are connecting to with this client have the {@code crud-router} role enabled.
 *
 * The default implementation of metadata retrieving function is provided by the DDL module
 * (<a href="https://github.com/tarantool/ddl">https://github.com/tarantool/ddl</a>). It is available by default
 * on the Cartridge instances. In the other cases, you'll have to expose the DDL module as public API on the target
 * Tarantool instance or use some other implementation of that function.
 *
 * See <a href="https://github.com/tarantool/examples/blob/master/profile-storage/README.md">
 *     https://github.com/tarantool/examples/blob/master/profile-storage/README.md</a>
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public class ProxyTarantoolClient implements TarantoolClient, ProxyOperationsMapping {

    private final TarantoolClient client;
    private final ProxyMetadataProvider metadataProvider;

    public ProxyTarantoolClient(TarantoolClient decoratedClient) {
        this.client = decoratedClient;
        this.client.getListeners().clear();
        this.metadataProvider = new ProxyMetadataProvider(
                client, getGetSchemaFunctionName(), new DDLTarantoolSpaceMetadataConverter());
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
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        return client.metadata();
    }

    @Override
    public TarantoolMetadataProvider metadataProvider() {
        return metadataProvider;
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
    public TarantoolConnectionListeners getListeners() {
        return this.client.getListeners();
    }

    @Override
    public TarantoolTupleFactory getTarantoolTupleFactory() {
        return this.client.getTarantoolTupleFactory();
    }

    @Override
    public ResultMapperFactoryFactory getResultMapperFactoryFactory() {
        return client.getResultMapperFactoryFactory();
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName) throws TarantoolClientException {
        return client.call(functionName);
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, Object... arguments)
            throws TarantoolClientException {
        return client.call(functionName, Arrays.asList(arguments));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, List<?> arguments)
            throws TarantoolClientException {
        return client.call(functionName, arguments);
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, List<?> arguments, MessagePackMapper mapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, mapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, Class<T> entityClass)
            throws TarantoolClientException {
        return client.call(functionName, entityClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, resultMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, List<?> arguments, Class<T> entityClass)
            throws TarantoolClientException {
        return client.call(functionName, arguments, entityClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, resultMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<?> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return client.call(functionName, arguments, argumentsMapper, entityClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return client.call(functionName, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            Class<T> resultClass)
            throws TarantoolClientException {
        return client.callForSingleResult(functionName, arguments, argumentsMapper, resultClass);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper)
            throws TarantoolClientException {
        return client.callForSingleResult(functionName, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName, List<?> arguments, Class<T> resultClass)
            throws TarantoolClientException {
        return client.callForSingleResult(functionName, arguments, resultClass);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(
            String functionName,
            List<?> arguments,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) throws TarantoolClientException {
        return client.callForSingleResult(functionName, arguments, resultMapper);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName, Class<T> resultClass)
            throws TarantoolClientException {
        return client.callForSingleResult(functionName, resultClass);
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(
            String functionName,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) throws TarantoolClientException {
        return client.callForSingleResult(functionName, resultMapper);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            Class<R> resultClass) throws TarantoolClientException {
        return client.callForMultiResult(functionName, arguments, argumentsMapper, resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) throws TarantoolClientException {
        return client.callForMultiResult(functionName, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          Class<R> resultClass)
            throws TarantoolClientException {
        return client.callForMultiResult(functionName, arguments, resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) throws TarantoolClientException {
        return client.callForMultiResult(functionName, arguments, resultMapper);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName, Class<R> resultClass)
            throws TarantoolClientException {
        return client.callForMultiResult(functionName, resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper)
            throws TarantoolClientException {
        return client.callForMultiResult(functionName, resultMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression) throws TarantoolClientException {
        return client.eval(expression);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments) throws TarantoolClientException {
        return client.eval(expression, arguments);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, resultMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return client.eval(expression, arguments, resultMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression,
                                           List<?> arguments,
                                           MessagePackObjectMapper argumentsMapper,
                                           MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return client.eval(expression, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
