package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Main class for connecting to a single Tarantool server. Provides basic API for interacting with the database
 * and manages connections.
 *
 * @author Alexey Kuzin
 */
public class StandaloneTarantoolClient implements TarantoolClient {

    private EventLoopGroup eventLoopGroup;
    private TarantoolClientConfig config;
    private Bootstrap bootstrap;
    private List<TarantoolConnection> connections;
    private TarantoolSingleAddressProvider addressProvider;
    private TarantoolMetadataOperations metadata;

    /**
     * Create a client. Default guest credentials will be used. Connects to a Tarantool server on localhost using the
     * default port (3301)
     */
    public StandaloneTarantoolClient() {
        this(new SimpleTarantoolCredentials());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server on localhost using
     * the default port (3301)
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials) {
       this(credentials, new TarantoolServerAddress());
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * host and port.
     * @param credentials Tarantool user credentials holder
     * @param host valid host name or IP address
     * @param port valid port number
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials, String host, int port) {
       this(credentials, new TarantoolServerAddress(host, port));
    }

    /**
     * Create a client using provided credentials information. Connects to a Tarantool server using the specified
     * server address.
     * @param credentials Tarantool user credentials holder
     * @param address Tarantool server address
     * @see TarantoolCredentials
     * @see TarantoolServerAddress
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials, TarantoolServerAddress address) {
       this(new TarantoolClientConfig.Builder()
               .withCredentials(credentials)
               .build(),
           () -> address);
    }

    /**
     * Create a client. The server address for connecting to the server is specified by the passed address provider.
     * @param config the client configuration
     * @param addressProvider provides Tarantool server address for connection
     * @see TarantoolClientConfig
     */
    public StandaloneTarantoolClient(TarantoolClientConfig config,
                                     TarantoolSingleAddressProvider addressProvider) {
        this.config = config;
        this.connections = new ArrayList<>(config.getConnections());
        this.addressProvider = addressProvider;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
        this.metadata = new TarantoolMetadata(config, this);
        connect();
    }

    //TODO invoke on reconnect
    private void connect() throws TarantoolClientException {
        try {
            InetSocketAddress serverAddress = addressProvider.getAddress().getSocketAddress();
            List<CompletableFuture<TarantoolConnection>> connectionFutures = new ArrayList<>(config.getConnections());
            for (int i = 0; i < config.getConnections(); i++) {
                connectionFutures.add(new TarantoolConnectionImpl(config, bootstrap.clone(), serverAddress).connect());
            }
            CompletableFuture.allOf(connectionFutures.toArray(new CompletableFuture[config.getConnections()]))
                    .thenApply(f -> {
                        connectionFutures.forEach(cf -> cf.thenApply(connections::add));
                        return f;
                    })
                    .thenApplyAsync(c -> {
                        try {
                            metadata().refresh().get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
                            return c;
                        } catch (Throwable e) {
                            throw new CompletionException(e);
                        }
                    })
                    .get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * Get the established connection for sending requests to the Tarantool server
     * @return Tarantool server connection
     */
    TarantoolConnection getConnection() {
        //TODO connection select strategy
        return connections.get(0);
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        return getConnection().getVersion();
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) throws TarantoolClientException {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        if (!getConnection().isConnected()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        Optional<TarantoolSpaceMetadata> meta = this.metadata().getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }
        return new TarantoolSpace(meta.get().getSpaceId(), config, getConnection(), metadata());
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        if (!getConnection().isConnected()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        return new TarantoolSpace(spaceId, config, getConnection(), metadata());
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        if (!getConnection().isConnected()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
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
            return getConnection().sendRequest(request, resultMapper);
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
            return getConnection().sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public TarantoolClientConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws Exception {
        try {
            for (TarantoolConnection conn: connections) {
                conn.close();
            }
        } finally {
            try {
                eventLoopGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
