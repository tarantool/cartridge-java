package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionFactory;
import io.tarantool.driver.core.TarantoolConnectionListeners;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import io.tarantool.driver.protocol.requests.TarantoolEvalRequest;
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
    private final TarantoolMetadata metadata;
    private final TarantoolClientConfig config;
    private final Bootstrap bootstrap;
    private final TarantoolConnectionFactory connectionFactory;
    private final TarantoolConnectionListeners listeners;
    private final AtomicReference<TarantoolConnectionManager> connectionManagerHolder = new AtomicReference<>();

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
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        this.metadata = new TarantoolMetadata(config, this);
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

        Optional<TarantoolSpaceMetadata> meta = this.metadata().getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }
        return new TarantoolSpace(meta.get().getSpaceId(), config, connectionManager(), metadata());
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        return new TarantoolSpace(spaceId, config, connectionManager(), metadata());
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        return metadata;
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
    public CompletableFuture<List<Object>> call(String functionName, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return call(functionName, Collections.emptyList(), config.getMessagePackMapper());
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments,
                                               MessagePackObjectMapper argumentsMapper,
                                               MessagePackValueMapper resultMapper) throws TarantoolClientException {
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
    public <T> CompletableFuture<List<T>> eval(String expression, MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, Collections.emptyList(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> eval(String expression,
                                               List<Object> arguments,
                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return eval(expression, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> eval(String expression, List<Object> arguments,
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
}
