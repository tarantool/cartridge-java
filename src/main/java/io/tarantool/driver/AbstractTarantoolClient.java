package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.api.CallResult;
import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnection;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.DefaultResultMapperFactoryFactory;
import io.tarantool.driver.mappers.DefaultSingleValueResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.MultiValueListConverter;
import io.tarantool.driver.mappers.ResultMapperFactoryFactory;
import io.tarantool.driver.mappers.SingleValueTarantoolResultMapperFactory;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.SpacesMetadataProvider;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import io.tarantool.driver.protocol.requests.TarantoolEvalRequest;
import io.tarantool.driver.utils.Assert;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Basic Tarantool client implementation. Subclasses must provide the connection manager.
 *
 * @param <T> target tuple type
 * @param <R> target tuple collection type
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolClient<T extends Packable, R extends Collection<T>>
        implements TarantoolClient<T, R> {

    private final NioEventLoopGroup eventLoopGroup;
    private final TarantoolClientConfig config;
    private final Bootstrap bootstrap;
    private final TarantoolConnectionFactory connectionFactory;
    private final TarantoolConnectionListeners listeners;
    private final AtomicReference<TarantoolConnectionManager> connectionManagerHolder = new AtomicReference<>();
    private final AtomicReference<TarantoolMetadata> metadataHolder = new AtomicReference<>();
    private final DefaultResultMapperFactoryFactory mapperFactoryFactory;
    private final SpacesMetadataProvider metadataProvider;

    /**
     * Create a client.
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    public AbstractTarantoolClient(TarantoolClientConfig config) {
        this(config, new TarantoolConnectionListeners());
    }

    /**
     * Create a client, specifying the connection established event listeners.
     * @deprecated
     * @param config the client configuration
     * @param selectionStrategyFactory instantiates strategies which provide the algorithm of selecting connections
     *                                 from the connection pool for performing the next request
     * @param listeners connection established event listeners
     * @see TarantoolClientConfig
     */
    protected AbstractTarantoolClient(TarantoolClientConfig config,
                                      ConnectionSelectionStrategyFactory selectionStrategyFactory,
                                      TarantoolConnectionListeners listeners) {
        this(config, listeners);
    }

    /**
     * Create a client, specifying the connection established event listeners.
     * @param config the client configuration
     * @param listeners connection established event listeners
     * @see TarantoolClientConfig
     */
    public AbstractTarantoolClient(TarantoolClientConfig config, TarantoolConnectionListeners listeners) {
        Assert.notNull(config, "Tarantool client config must not be null");
        Assert.notNull(listeners, "Tarantool connection listeners must not be null");

        this.config = config;
        this.mapperFactoryFactory = new DefaultResultMapperFactoryFactory();
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        this.connectionFactory = new TarantoolConnectionFactory(config, this.bootstrap);
        this.listeners = listeners;
        this.metadataProvider = new SpacesMetadataProvider(this, config.getMessagePackMapper());
    }

    /**
     * Provides a connection manager for Tarantool server connections
     * @param config contains Tarantool client configuration options
     * @param connectionFactory provides helper methods for connection instantiation
     * @param listeners listeners which will be invoked once all connections are established
     * @return connection manager
     */
    protected abstract TarantoolConnectionManager connectionManager(TarantoolClientConfig config,
                                                                    TarantoolConnectionFactory connectionFactory,
                                                                    TarantoolConnectionListeners listeners);

    private TarantoolConnectionManager connectionManager() {
        if (this.connectionManagerHolder.get() == null) {
            this.connectionManagerHolder.compareAndSet(null, connectionManager(config, connectionFactory, listeners));
        }
        return connectionManagerHolder.get();
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        try {
            return connectionManager().getConnection().thenApply(TarantoolConnection::getVersion).get();
        } catch (InterruptedException e) {
            throw new TarantoolClientException(e);
        } catch (ExecutionException e) {
            throw new TarantoolClientException(e.getCause());
        }
    }

    @Override
    public TarantoolSpaceOperations<T, R> space(String spaceName) throws TarantoolClientException {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }

        return spaceOperations(config, connectionManager(), metadata, meta.get());
    }

    @Override
    public TarantoolSpaceOperations<T, R> space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceById(spaceId);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceId);
        }

        return spaceOperations(config, connectionManager(), metadata, meta.get());
    }

    /**
     * Creates a space API implementation instance for the specified space
     *
     * @param config Tarantool client configuration
     * @param connectionManager configured internal connection manager
     * @param metadata metadata operations
     * @param spaceMetadata current space metadata
     * @return space API implementation instance
     */
    protected abstract TarantoolSpaceOperations<T, R> spaceOperations(TarantoolClientConfig config,
                                                                      TarantoolConnectionManager connectionManager,
                                                                      TarantoolMetadataOperations metadata,
                                                                      TarantoolSpaceMetadata spaceMetadata);

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        if (metadataHolder.get() == null) {
            this.metadataHolder.compareAndSet(null, new TarantoolMetadata(metadataProvider()));
        }
        return metadataHolder.get();
    }

    @Override
    public TarantoolMetadataProvider metadataProvider() {
        return metadataProvider;
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName) throws TarantoolClientException {
        return call(functionName, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, Object... arguments)
            throws TarantoolClientException {
        return call(functionName, Arrays.asList(arguments));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, List<?> arguments)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, List<?> arguments, MessagePackMapper mapper)
            throws TarantoolClientException {
        return makeRequest(functionName, arguments, mapper, mapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, Class<T> tupleClass)
            throws TarantoolClientException {
        return call(functionName, Collections.emptyList(), tupleClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return call(functionName, Collections.emptyList(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, List<?> arguments, Class<T> tupleClass)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), tupleClass);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<?> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          Class<T> tupleClass)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, T> converter = getArrayValueConverter(tupleClass);
        return call(functionName, arguments, argumentsMapper,
                getMapperFactory(tupleClass).withTarantoolResultConverter(converter));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, argumentsMapper, resultMapper);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(String functionName,
                                                        List<?> arguments,
                                                        Class<S> resultClass)
            throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, config.getMessagePackMapper(), resultClass);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
            String functionName,
            List<?> arguments,
            CallResultMapper<S, SingleValueCallResult<S>> resultMapper) throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(String functionName, Class<S> resultClass)
            throws TarantoolClientException {
        return callForSingleResult(functionName, Collections.emptyList(), resultClass);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
            String functionName,
            CallResultMapper<S, SingleValueCallResult<S>> resultMapper) throws TarantoolClientException {
        return callForSingleResult(functionName, Collections.emptyList(), resultMapper);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            Class<S> resultClass)
            throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, argumentsMapper, getDefaultSingleValueMapper(resultClass));
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<S, SingleValueCallResult<S>> resultMapper)
            throws TarantoolClientException {
        return makeRequestForSingleResult(functionName, arguments, argumentsMapper, resultMapper)
                .thenApply(CallResult::value);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          Class<R> resultClass)
            throws TarantoolClientException {
        return callForMultiResult(functionName, arguments, config.getMessagePackMapper(), resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) throws TarantoolClientException {
        return callForMultiResult(functionName, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName, Class<R> resultClass)
            throws TarantoolClientException {
        return callForMultiResult(functionName, Collections.emptyList(), resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper)
            throws TarantoolClientException {
        return callForMultiResult(functionName, Collections.emptyList(), resultMapper);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(String functionName,
                                                                          List<?> arguments,
                                                                          MessagePackObjectMapper argumentsMapper,
                                                                          Class<R> resultClass)
            throws TarantoolClientException {
        return callForMultiResult(functionName, arguments, argumentsMapper, withResultClass(resultClass));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper)
            throws TarantoolClientException {
        return makeRequestForMultiResult(functionName, arguments, argumentsMapper, resultMapper)
                .thenApply(CallResult::value);
    }

    private <T> CompletableFuture<CallResult<T>> makeRequestForSingleResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        return makeRequest(functionName, arguments, argumentsMapper, resultMapper);
    }

    private <T, R extends List<T>> CompletableFuture<CallResult<R>> makeRequestForMultiResult(
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<R, MultiValueCallResult<T, R>> resultMapper) {
        return makeRequest(functionName, arguments, argumentsMapper, resultMapper);
    }

    private <S> CompletableFuture<S> makeRequest(String functionName,
                                                 List<?> arguments,
                                                 MessagePackObjectMapper argumentsMapper,
                                                 MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
            TarantoolCallRequest.Builder builder = new TarantoolCallRequest.Builder()
                .withFunctionName(functionName);

            if (arguments.size() > 0) {
                builder.withArguments(arguments);
            }

            TarantoolCallRequest request = builder.build(argumentsMapper);
            return connectionManager().getConnection().thenCompose(c -> c.sendRequest(request, resultMapper));
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private
    <T, R extends List<T>> CallResultMapper<R, MultiValueCallResult<T, R>>
    withResultClass(Class<R> resultClass) {
        return mapperFactoryFactory.multiValueResultMapperFactory(resultClass)
                .withMultiValueResultConverter(
                        (ValueConverter<ArrayValue, R>) new MultiValueListConverter<>(getValueConverter(resultClass)));
    }

    private <T> SingleValueTarantoolResultMapperFactory<T> getMapperFactory(Class<T> resultClass) {
        return mapperFactoryFactory.singleValueTarantoolResultMapperFactory(resultClass);
    }

    @SuppressWarnings("unchecked")
    private <T> CallResultMapper<T, SingleValueCallResult<T>> getDefaultSingleValueMapper(Class<T> tupleClass) {
        return new DefaultSingleValueResultMapper<>(config.getMessagePackMapper(), tupleClass);
    }

    @SuppressWarnings("unchecked")
    private <T> ValueConverter<Value, T> getValueConverter(Class<T> tupleClass) {
        return (ValueConverter<Value, T>) getConverter(Value.class, tupleClass);
    }

    @SuppressWarnings("unchecked")
    private <T> ValueConverter<ArrayValue, T> getArrayValueConverter(Class<T> tupleClass) {
        return (ValueConverter<ArrayValue, T>) getConverter(ArrayValue.class, tupleClass);
    }

    private <T> ValueConverter<? extends Value, T> getConverter(Class<? extends Value> valueClass,
                                                                Class<T> tupleClass) {
        Optional<? extends ValueConverter<? extends Value, T>> converter =
                config.getMessagePackMapper().getValueConverter(valueClass, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException(
                    "No converter for value class %s and type %s is present", valueClass, tupleClass);
        }
        return converter.get();
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression) throws TarantoolClientException {
        return eval(expression, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments)
            throws TarantoolClientException {
        return eval(expression, arguments, config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, Collections.emptyList(), resultMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, List<?> arguments, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression,
                                           List<?> arguments,
                                           MessagePackObjectMapper argumentsMapper,
                                           MessagePackValueMapper resultMapper) throws TarantoolClientException {
        try {
            TarantoolEvalRequest request = new TarantoolEvalRequest.Builder()
                    .withExpression(expression)
                    .withArguments(arguments)
                    .build(argumentsMapper);
            return connectionManager().getConnection().thenCompose(c -> c.sendRequest(request, resultMapper));
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public TarantoolClientConfig getConfig() {
        return config;
    }

    protected Bootstrap getBootstrap() {
        return bootstrap;
    }

    @Override
    public void close() throws Exception {
        try {
            connectionManager().close();
        } finally {
            try {
                eventLoopGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public TarantoolConnectionListeners getListeners() {
        return listeners;
    }

    @Override
    public ResultMapperFactoryFactory getResultMapperFactoryFactory() {
        return mapperFactoryFactory;
    }
}
