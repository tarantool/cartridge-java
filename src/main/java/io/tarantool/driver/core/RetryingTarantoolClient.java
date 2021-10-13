package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.core.space.RetryingTarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ResultMapperFactoryFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.api.retry.RequestRetryPolicy;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import org.msgpack.value.Value;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Client implementation that decorates a {@link TarantoolClient} instance, allowing to specify a retry policy for
 * all requests made through this client instance.
 * <p>
 * Retry policy is applied before the possible exception is propagated to the user in the wrapping CompletableFuture.
 * Since that, the timeout specified for waiting the future result, bounds externally the overall operation time.
 *
 * @param <T> target tuple type
 * @param <R> target tuple collection type
 * @author Alexey Kuzin
 */
public abstract class RetryingTarantoolClient<T extends Packable, R extends Collection<T>>
        implements TarantoolClient<T, R> {

    private final TarantoolClient<T, R> client;
    private final RequestRetryPolicyFactory retryPolicyFactory;
    private final Executor executor;

    /**
     * Basic constructor. {@link Executors#newWorkStealingPool()} is used for executor by default.
     *
     * @param decoratedClient    configured Tarantool client
     * @param retryPolicyFactory request retrying policy settings
     */
    public RetryingTarantoolClient(TarantoolClient<T, R> decoratedClient,
                                   RequestRetryPolicyFactory retryPolicyFactory) {
        this(decoratedClient, retryPolicyFactory, Executors.newWorkStealingPool());
    }

    /**
     * Basic constructor
     *
     * @param decoratedClient    configured Tarantool client
     * @param retryPolicyFactory request retrying policy settings
     * @param executor           executor service for retry callbacks
     */
    public RetryingTarantoolClient(TarantoolClient<T, R> decoratedClient,
                                   RequestRetryPolicyFactory retryPolicyFactory,
                                   Executor executor) {
        this.client = decoratedClient;
        this.retryPolicyFactory = retryPolicyFactory;
        this.executor = executor;
    }

    @Override
    public TarantoolMetadataProvider metadataProvider() {
        return client.metadataProvider();
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
    public TarantoolSpaceOperations<T, R> space(String spaceName) throws TarantoolClientException {
        return spaceOperations(client.space(spaceName), retryPolicyFactory, executor);
    }

    @Override
    public TarantoolSpaceOperations<T, R> space(int spaceId) throws TarantoolClientException {
        return spaceOperations(client.space(spaceId), retryPolicyFactory, executor);
    }

    /**
     * Creates a space API implementation instance for the specified space
     *
     * @param decoratedSpaceOperations space API implementation form the decorated Tarantool client instance
     * @param retryPolicyFactory       request retrying policy factory
     * @param executor                 executor service for retry callbacks
     * @return space API implementation instance
     */
    protected abstract RetryingTarantoolSpace<T, R> spaceOperations(
            TarantoolSpaceOperations<T, R> decoratedSpaceOperations,
            RequestRetryPolicyFactory retryPolicyFactory,
            Executor executor);

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        return client.metadata();
    }

    @Override
    public TarantoolConnectionListeners getConnectionListeners() {
        return client.getConnectionListeners();
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, Object... arguments) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, List<?> arguments) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName,
                                           List<?> arguments,
                                           MessagePackMapper mapper) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments, mapper));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, entityClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, resultMapper));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<?> arguments,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments, entityClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return null;
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<?> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          Class<T> entityClass) throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments, argumentsMapper, entityClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.call(functionName, arguments, argumentsMapper, resultMapper));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        MessagePackObjectMapper argumentsMapper,
                                                        Class<T> resultClass) throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, arguments, argumentsMapper, resultClass));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        MessagePackObjectMapper argumentsMapper,
                                                        ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() ->
                client.callForSingleResult(functionName, arguments, argumentsMapper, valueConverter));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        MessagePackObjectMapper argumentsMapper,
                                                        CallResultMapper<T, SingleValueCallResult<T>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, arguments, argumentsMapper, resultMapper));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName, List<?> arguments, Class<T> resultClass)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, arguments, resultClass));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, arguments, valueConverter));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        CallResultMapper<T, SingleValueCallResult<T>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, arguments, resultMapper));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName, Class<T> resultClass)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, resultClass));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName, ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, valueConverter));
    }

    @Override
    public <T> CompletableFuture<T> callForSingleResult(String functionName,
                                                        CallResultMapper<T, SingleValueCallResult<T>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForSingleResult(functionName, resultMapper));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          MessagePackObjectMapper argumentsMapper,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          Class<T> resultClass)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(
                functionName, arguments, argumentsMapper, resultContainerSupplier, resultClass));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          MessagePackObjectMapper argumentsMapper,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(
                functionName, arguments, argumentsMapper, resultContainerSupplier, valueConverter));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(functionName, arguments, argumentsMapper, resultMapper));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          Class<T> resultClass)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(
                functionName, arguments, resultContainerSupplier, resultClass));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(
                functionName, arguments, resultContainerSupplier, valueConverter));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(functionName, arguments, resultMapper));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          Class<T> resultClass)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(functionName, resultContainerSupplier, resultClass));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          Supplier<R> resultContainerSupplier,
                                                                          ValueConverter<Value, T> valueConverter)
            throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(functionName, resultContainerSupplier, valueConverter));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) throws TarantoolClientException {
        return wrapOperation(() -> client.callForMultiResult(functionName, resultMapper));
    }

    @Override
    public ResultMapperFactoryFactory getResultMapperFactoryFactory() {
        return client.getResultMapperFactoryFactory();
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression) throws TarantoolClientException {
        return wrapOperation(() -> client.eval(expression));
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments) throws TarantoolClientException {
        return wrapOperation(() -> client.eval(expression, arguments));
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression,
                                           MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return wrapOperation(() -> client.eval(expression, resultMapper));
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression,
                                           List<?> arguments,
                                           MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return wrapOperation(() -> client.eval(expression, arguments, resultMapper));
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments,
                                           MessagePackObjectMapper argumentsMapper,
                                           MessagePackValueMapper resultMapper) throws TarantoolClientException {
        return wrapOperation(() -> client.eval(expression, arguments, argumentsMapper, resultMapper));
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    /**
     * Getter for {@link RequestRetryPolicyFactory}
     *
     * @return {@link RequestRetryPolicyFactory}
     */
    protected RequestRetryPolicyFactory getRetryPolicyFactory() {
        return retryPolicyFactory;
    }

    /**
     * Getter for decorated client
     *
     * @return decorated client {@link TarantoolClient}
     */
    protected TarantoolClient<T, R> getClient() {
        return client;
    }

    private <S> CompletableFuture<S> wrapOperation(Supplier<CompletableFuture<S>> operation) {
        RequestRetryPolicy retryPolicy = retryPolicyFactory.create();
        return retryPolicy.wrapOperation(operation, executor);
    }
}
