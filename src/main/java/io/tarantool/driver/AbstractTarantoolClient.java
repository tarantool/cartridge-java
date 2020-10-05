package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import io.tarantool.driver.protocol.requests.TarantoolEvalRequest;
import org.msgpack.value.ArrayValue;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Basic Tarantool client implementation. Subclasses must provide the connection manager.
 *
 * @author Alexey Kuzin
 */
public abstract class AbstractTarantoolClient implements TarantoolClient {

    private final NioEventLoopGroup eventLoopGroup;
    private final TarantoolClientConfig config;
    private final Bootstrap bootstrap;
    private final TarantoolConnectionFactory connectionFactory;
    private final TarantoolConnectionListeners listeners;
    private final AtomicReference<TarantoolConnectionManager> connectionManagerHolder = new AtomicReference<>();
    private final AtomicReference<TarantoolMetadata> metadataHolder = new AtomicReference<>();
    private final TarantoolCallResultMapperFactory mapperFactory;

    /**
     * Create a client.
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    protected AbstractTarantoolClient(TarantoolClientConfig config) {
        this(config, new TarantoolConnectionListeners());
    }

    /**
     * Create a client, specifying the connection established event listeners.
     * @param listeners connection established event listeners
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    protected AbstractTarantoolClient(TarantoolClientConfig config, TarantoolConnectionListeners listeners) {
        this.config = config;
        this.mapperFactory = new TarantoolCallResultMapperFactory(config.getMessagePackMapper());
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        this.connectionFactory = new TarantoolConnectionFactory(config, getBootstrap());
        listeners.add(connection -> {
            try {
                return metadata().refresh().thenApply(v -> connection);
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        });
        this.listeners = listeners;
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
        return connectionManager().getConnection().getVersion();
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) throws TarantoolClientException {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }

        return new TarantoolSpace(config, connectionManager(), meta.get(), metadata);
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        TarantoolMetadataOperations metadata = this.metadata();
        Optional<TarantoolSpaceMetadata> meta = metadata.getSpaceById(spaceId);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceId);
        }

        return new TarantoolSpace(config, connectionManager(), meta.get(), metadata());
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        if (metadataHolder.get() == null) {
            this.metadataHolder.compareAndSet(null, new TarantoolMetadata(config, connectionManager()));
        }
        return metadataHolder.get();
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName) throws TarantoolClientException {
        return call(functionName, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName, List<Object> arguments)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName,
                                                List<Object> arguments,
                                                MessagePackMapper mapper)
            throws TarantoolClientException {
        try {
            TarantoolCallRequest.Builder builder = new TarantoolCallRequest.Builder()
                    .withFunctionName(functionName);

            if (arguments.size() > 0) {
                builder.withArguments(arguments);
            }

            TarantoolCallRequest request = builder.build(mapper);
            return connectionManager().getConnection().sendRequest(request, mapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName, Class<T> tupleClass)
            throws TarantoolClientException {
        return call(functionName, getConverter(tupleClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return call(functionName, Collections.emptyList(), tupleMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          Class<T> tupleClass)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), getConverter(tupleClass));
    }
    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), tupleMapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          Class<T> tupleClass)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, T> converter = getConverter(tupleClass);
        return call(functionName, arguments, argumentsMapper, mapperFactory.withConverter(tupleClass, converter));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return call(functionName, arguments, argumentsMapper, mapperFactory.withConverter(tupleMapper));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> call(String functionName,
                                                          List<Object> arguments,
                                                          MessagePackObjectMapper argumentsMapper,
                                                          TarantoolCallResultMapper<T> resultMapper) {
        try {
            TarantoolCallRequest.Builder builder = new TarantoolCallRequest.Builder()
                    .withFunctionName(functionName);

            if (arguments.size() > 0) {
                builder.withArguments(arguments);
            }

            TarantoolCallRequest request = builder.build(argumentsMapper);
            return connectionManager().getConnection().sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    private <T> ValueConverter<ArrayValue, T> getConverter(Class<T> tupleClass) {
        Optional<ValueConverter<ArrayValue, T>> converter =
                config.getMessagePackMapper().getValueConverter(ArrayValue.class, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException("No ArrayValue converter for type " + tupleClass + " is present");
        }
        return converter.get();
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression) throws TarantoolClientException {
        return eval(expression, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression, List<Object> arguments)
            throws TarantoolClientException {
        return eval(expression, arguments, config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, Collections.emptyList(), resultMapper);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression,
                                                List<Object> arguments,
                                                MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public CompletableFuture<List<Object>> eval(String expression,
                                                List<Object> arguments,
                                                MessagePackObjectMapper argumentsMapper,
                                                MessagePackValueMapper resultMapper) throws TarantoolClientException {
        try {
            TarantoolEvalRequest request = new TarantoolEvalRequest.Builder()
                    .withExpression(expression)
                    .withArguments(arguments)
                    .build(argumentsMapper);
            return connectionManager().getConnection().sendRequest(request, resultMapper);
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
}
