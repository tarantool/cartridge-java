package io.tarantool.driver.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.msgpack.value.Value;

import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.api.CallResult;
import io.tarantool.driver.api.MultiValueCallResult;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionListeners;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.connection.TarantoolConnectionFactory;
import io.tarantool.driver.core.connection.TarantoolConnectionManager;
import io.tarantool.driver.core.metadata.SpacesMetadataProvider;
import io.tarantool.driver.core.metadata.TarantoolMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactoryImpl;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequestSignature;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import io.tarantool.driver.protocol.requests.TarantoolEvalRequest;
import io.tarantool.driver.utils.Assert;

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
    private final AtomicReference<TarantoolMetadata> metadataHolder = new AtomicReference<>();
    private final ResultMapperFactoryFactoryImpl mapperFactoryFactory;
    private final Map<TarantoolRequestSignature, MessagePackObjectMapper> argumentsMapperCache;
    private final Map<TarantoolRequestSignature, MessagePackValueMapper> resultMapperCache;

    private final SpacesMetadataProvider metadataProvider;
    private final ScheduledExecutorService timeoutScheduler;
    private TarantoolConnectionManager connectionManager;

    /**
     * Create a client.
     *
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    public AbstractTarantoolClient(TarantoolClientConfig config) {
        this(config, new TarantoolConnectionListeners());
    }

    /**
     * Create a client, specifying the connection established event listeners.
     *
     * @param config                   the client configuration
     * @param selectionStrategyFactory instantiates strategies which provide the algorithm of selecting connections
     *                                 from the connection pool for performing the next request
     * @param listeners                connection established event listeners
     * @see TarantoolClientConfig
     * @deprecated
     */
    protected AbstractTarantoolClient(
        TarantoolClientConfig config,
        ConnectionSelectionStrategyFactory selectionStrategyFactory,
        TarantoolConnectionListeners listeners) {
        this(config, listeners);
    }

    /**
     * Create a client, specifying the connection established event listeners.
     *
     * @param config    the client configuration
     * @param listeners connection established event listeners
     * @see TarantoolClientConfig
     */
    public AbstractTarantoolClient(TarantoolClientConfig config, TarantoolConnectionListeners listeners) {
        Assert.notNull(config, "Tarantool client config must not be null");
        Assert.notNull(listeners, "Tarantool connection listeners must not be null");

        this.config = config;
        this.mapperFactoryFactory = new ResultMapperFactoryFactoryImpl();
        this.argumentsMapperCache = new ConcurrentHashMap<>();
        this.resultMapperCache = new ConcurrentHashMap<>();
        this.eventLoopGroup = new NioEventLoopGroup(config.getEventLoopThreadsNumber());
        this.bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        this.timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(new TarantoolDaemonThreadFactory("tarantool-timeout"));
        this.connectionFactory = new TarantoolConnectionFactory(config, this.bootstrap, this.timeoutScheduler);
        this.listeners = listeners;
        this.metadataProvider = new SpacesMetadataProvider(this, config.getMessagePackMapper());
    }

    /**
     * Provides a connection manager for Tarantool server connections
     *
     * @param config            contains Tarantool client configuration options
     * @param connectionFactory provides helper methods for connection instantiation
     * @param listeners         listeners which will be invoked once all connections are established
     * @return connection manager
     */
    protected abstract TarantoolConnectionManager connectionManager(
        TarantoolClientConfig config,
        TarantoolConnectionFactory connectionFactory,
        TarantoolConnectionListeners listeners);

    private TarantoolConnectionManager connectionManager() {
        if (this.connectionManager == null) {
            synchronized (this) {
                if (this.connectionManager == null) {
                    this.connectionManager = connectionManager(config, connectionFactory, listeners);
                }
            }
        }
        return this.connectionManager;
    }

    @Override
    public boolean refresh() {
        return connectionManager().refresh();
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
     * @param config            Tarantool client configuration
     * @param connectionManager configured internal connection manager
     * @param metadata          metadata operations
     * @param spaceMetadata     current space metadata
     * @return space API implementation instance
     */
    protected abstract TarantoolSpaceOperations<T, R> spaceOperations(
        TarantoolClientConfig config,
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
        return makeRequest(functionName, Collections.emptyList(), null,
            config::getMessagePackMapper, config::getMessagePackMapper);
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, Object... arguments)
        throws TarantoolClientException {
        return call(functionName, Arrays.asList(arguments));
    }

    @Override
    public CompletableFuture<List<?>> call(String functionName, Collection<?> arguments)
        throws TarantoolClientException {
        return makeRequest(functionName, arguments, null,
            config::getMessagePackMapper, config::getMessagePackMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(String functionName, Class<T> tupleClass)
        throws TarantoolClientException {
        return callForTupleResult(functionName, Collections.emptyList(), tupleClass);
    }

    @Override
    public <T> CompletableFuture<T> call(
        String functionName,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException {
        return call(functionName, Collections.emptyList(), resultMapperSupplier);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(
            String functionName, Collection<?> arguments, Class<T> tupleClass)
        throws TarantoolClientException {
        return callForTupleResult(functionName, arguments, config::getMessagePackMapper, tupleClass);
    }

    @Override
    public <T> CompletableFuture<T> call(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException {
        return call(functionName, arguments, config::getMessagePackMapper, resultMapperSupplier);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> callForTupleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Class<T> tupleClass)
        throws TarantoolClientException {
        TarantoolRequestSignature signature = TarantoolRequestSignature.create(functionName, arguments, tupleClass);
        return callForSingleResult(functionName, arguments, signature, argumentsMapperSupplier,
            () -> mapperFactoryFactory.getTarantoolResultMapper(config.getMessagePackMapper(), tupleClass));
    }

    @Override
    public <T> CompletableFuture<T> call(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier)
        throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, argumentsMapperSupplier, resultMapperSupplier);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Class<S> resultClass)
        throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, config::getMessagePackMapper, resultClass);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        ValueConverter<Value, S> valueConverter)
        throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, config::getMessagePackMapper, valueConverter);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<S, SingleValueCallResult<S>>> resultMapperSupplier) throws TarantoolClientException {
        return callForSingleResult(functionName, arguments, config::getMessagePackMapper, resultMapperSupplier);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(String functionName, Class<S> resultClass)
        throws TarantoolClientException {
        return callForSingleResult(functionName, Collections.emptyList(), resultClass);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(String functionName, ValueConverter<Value, S> valueConverter)
        throws TarantoolClientException {
        return callForSingleResult(functionName, Collections.emptyList(), valueConverter);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Supplier<CallResultMapper<S, SingleValueCallResult<S>>> resultMapperSupplier) throws TarantoolClientException {
        return callForSingleResult(functionName, Collections.emptyList(), resultMapperSupplier);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Class<S> resultClass)
        throws TarantoolClientException {
        TarantoolRequestSignature signature = TarantoolRequestSignature.create(functionName, arguments, resultClass);
        return callForSingleResult(
            functionName, arguments, signature, argumentsMapperSupplier,
                () -> mapperFactoryFactory.getDefaultSingleValueMapper(config.getMessagePackMapper(), resultClass));
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        ValueConverter<Value, S> valueConverter)
        throws TarantoolClientException {
        Supplier<CallResultMapper<S, SingleValueCallResult<S>>> resultMapperSupplier = () ->
            mapperFactoryFactory.getSingleValueResultMapper(valueConverter);
        TarantoolRequestSignature signature = TarantoolRequestSignature.create(
            functionName, arguments, valueConverter.getClass());
        return callForSingleResult(functionName, arguments, signature, argumentsMapperSupplier, resultMapperSupplier);
    }

    private <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        TarantoolRequestSignature signature,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<S, SingleValueCallResult<S>>> resultMapperSupplier)
        throws TarantoolClientException {
        return makeRequestForSingleResult(
            functionName, arguments, signature, argumentsMapperSupplier, resultMapperSupplier)
            .thenApply(CallResult::value);
    }

    @Override
    public <S> CompletableFuture<S> callForSingleResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<S, SingleValueCallResult<S>>> resultMapperSupplier)
        throws TarantoolClientException {
        return makeRequestForSingleResult(
            functionName, arguments, null, argumentsMapperSupplier, resultMapperSupplier)
            .thenApply(CallResult::value);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException {
        return callForMultiResult(
            functionName, arguments, config::getMessagePackMapper, resultContainerSupplier, resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter) throws TarantoolClientException {
        return callForMultiResult(functionName, arguments, config::getMessagePackMapper,
            resultContainerSupplier, valueConverter);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException {
        return callForMultiResult(functionName, arguments, config::getMessagePackMapper, resultMapperSupplier);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException {
        return callForMultiResult(functionName, Collections.emptyList(), resultContainerSupplier, resultClass);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException {
        return callForMultiResult(functionName, Collections.emptyList(), resultContainerSupplier, valueConverter);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException {
        return callForMultiResult(functionName, Collections.emptyList(), resultMapperSupplier);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<R> resultContainerSupplier,
        Class<T> resultClass)
        throws TarantoolClientException {
        TarantoolRequestSignature signature = TarantoolRequestSignature.create(functionName, arguments, resultClass);
        return callForMultiResult(functionName, arguments, signature, argumentsMapperSupplier,
            () -> mapperFactoryFactory.getDefaultMultiValueMapper(config.getMessagePackMapper(), resultClass));
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<R> resultContainerSupplier,
        ValueConverter<Value, T> valueConverter)
        throws TarantoolClientException {
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier = () ->
            mapperFactoryFactory.getMultiValueResultMapper(resultContainerSupplier, valueConverter);
        TarantoolRequestSignature signature = TarantoolRequestSignature.create(
            functionName, arguments, valueConverter.getClass());
        return callForMultiResult(functionName, arguments, signature, argumentsMapperSupplier, resultMapperSupplier);
    }

    private <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        TarantoolRequestSignature signature,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException {
        return makeRequestForMultiResult(
            functionName, arguments, signature, argumentsMapperSupplier, resultMapperSupplier)
            .thenApply(CallResult::value);
    }

    @Override
    public <T, R extends List<T>> CompletableFuture<R> callForMultiResult(
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier)
        throws TarantoolClientException {
        return makeRequestForMultiResult(functionName, arguments, null, argumentsMapperSupplier, resultMapperSupplier)
            .thenApply(CallResult::value);
    }

    private <T> CompletableFuture<CallResult<T>> makeRequestForSingleResult(
        String functionName,
        Collection<?> arguments,
        TarantoolRequestSignature requestSignature,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
        return makeRequest(functionName, arguments, requestSignature, argumentsMapperSupplier, resultMapperSupplier);
    }

    private <T, R extends List<T>> CompletableFuture<CallResult<R>> makeRequestForMultiResult(
        String functionName,
        Collection<?> arguments,
        TarantoolRequestSignature requestSignature,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<R, MultiValueCallResult<T, R>>> resultMapperSupplier) {
        return makeRequest(functionName, arguments, requestSignature, argumentsMapperSupplier, resultMapperSupplier);
    }

    private <S> CompletableFuture<S> makeRequest(
        String functionName,
        Collection<?> arguments,
        TarantoolRequestSignature requestSignature,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<? extends MessagePackValueMapper> resultMapperSupplier)
        throws TarantoolClientException {
        try {
            TarantoolCallRequest.Builder builder = new TarantoolCallRequest.Builder()
                .withFunctionName(functionName);

            if (arguments.size() > 0) {
                builder.withArguments(arguments);
            }
            builder.withSignature(requestSignature);
            MessagePackObjectMapper argumentsMapper = requestSignature != null ?
                argumentsMapperCache.computeIfAbsent(requestSignature, s -> argumentsMapperSupplier.get()) :
                argumentsMapperSupplier.get();
            MessagePackValueMapper resultMapper = requestSignature != null ?
                resultMapperCache.computeIfAbsent(requestSignature, s -> resultMapperSupplier.get()) :
                resultMapperSupplier.get();

            TarantoolCallRequest request = builder.build(argumentsMapper);
            return connectionManager().getConnection()
                .thenCompose(c -> c.sendRequest(request).getFuture())
                .thenApply(resultMapper::fromValue);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression) throws TarantoolClientException {
        return eval(expression, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, Collection<?> arguments)
        throws TarantoolClientException {
        return eval(expression, arguments, config::getMessagePackMapper);
    }

    @Override
    public CompletableFuture<List<?>> eval(String expression, Supplier<MessagePackValueMapper> resultMapperSupplier)
        throws TarantoolClientException {
        return eval(expression, Collections.emptyList(), resultMapperSupplier);
    }

    @Override
    public CompletableFuture<List<?>> eval(
        String expression, Collection<?> arguments, Supplier<MessagePackValueMapper> resultMapperSupplier)
        throws TarantoolClientException {
        return eval(expression, arguments, config::getMessagePackMapper, resultMapperSupplier);
    }

    @Override
    public CompletableFuture<List<?>> eval(
        String expression,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<MessagePackValueMapper> resultMapperSupplier) throws TarantoolClientException {
        try {
            TarantoolRequestSignature signature = TarantoolRequestSignature.create(expression, arguments, List.class);
            MessagePackObjectMapper argumentsMapper = argumentsMapperCache.computeIfAbsent(
                signature, s -> argumentsMapperSupplier.get());
            MessagePackValueMapper resultMapper = resultMapperCache.computeIfAbsent(
                signature, s -> resultMapperSupplier.get());
            TarantoolEvalRequest request = new TarantoolEvalRequest.Builder()
                .withExpression(expression)
                .withArguments(arguments)
                .withSignature(signature)
                .build(argumentsMapper);
            return connectionManager().getConnection()
                .thenCompose(c -> c.sendRequest(request).getFuture())
                .thenApply(resultMapper::fromValue);
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
            timeoutScheduler.shutdownNow();
        } finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    @Override
    public TarantoolConnectionListeners getConnectionListeners() {
        return listeners;
    }

    @Override
    public ResultMapperFactoryFactory getResultMapperFactoryFactory() {
        return mapperFactoryFactory;
    }
}
