package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolChannelInitializer;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
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

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 3301;
    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "password";
    private static final int DEFAULT_CONNECT_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_READ_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_REQUEST_TIMEOUT = 2000; // milliseconds

    private EventLoopGroup eventLoopGroup;
    private TarantoolClientConfig config;
    private Bootstrap bootstrap;
    private ConcurrentHashMap<InetSocketAddress, TarantoolConnection> connections;

    /**
     * Create a client. Default credentials will be used.
     * @see InetSocketAddress
     */
    public StandaloneTarantoolClient() {
        this(new SimpleTarantoolCredentials(DEFAULT_USER, DEFAULT_PASSWORD));
    }

    /**
     * Create a client using provided credentials information.
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials) {
       this(new TarantoolClientConfig.Builder()
               .withCredentials(credentials)
               .withReadTimeout(DEFAULT_READ_TIMEOUT)
               .withConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
               .withRequestTimeout(DEFAULT_REQUEST_TIMEOUT)
               .build());
    }

    /**
     * Create a client.
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    public StandaloneTarantoolClient(TarantoolClientConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup();
        this.connections = new ConcurrentHashMap<>();
        this.bootstrap = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeout());
    }

    /**
     * Connect to a Tarantool server on localhost using the default port (3301)
     * @return connected client
     * @throws TarantoolClientException when connection or request for metadata are failed
     */
    public TarantoolConnection connect() throws TarantoolClientException {
        return connect(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Connect to a Tarantool server using the specified host and the default port (3301).
     * @param host valid host name or IP address
     * @return connected client
     * @see InetSocketAddress
     * @throws TarantoolClientException when connection or request for metadata are failed
     */
    public TarantoolConnection connect(String host) throws TarantoolClientException {
        return connect(host, DEFAULT_PORT);
    }

    /**
     * Connect to a Tarantool server using the specified host and port.
     * @param host valid host name or IP address
     * @param port valid port
     * @return connected client
     * @see InetSocketAddress
     * @throws TarantoolClientException when connection or request for metadata are failed
     */
    public TarantoolConnection connect(String host, int port) throws TarantoolClientException {
        return connect(new InetSocketAddress(host, port));
    }

    @Override
    public TarantoolConnection connect(InetSocketAddress address) throws TarantoolClientException {
        TarantoolConnection conn = connections.get(address); // TODO pool of multiple connections
        if (conn == null || conn.isClosed()) {
            CompletableFuture<Channel> connectionFuture = new CompletableFuture<>();
            RequestFutureManager futureManager = new RequestFutureManager(config);
            TarantoolVersionHolder versionHolder = new TarantoolVersionHolder();
            ChannelFuture future = bootstrap.clone()
                    .handler(new TarantoolChannelInitializer(config, futureManager, versionHolder, connectionFuture))
                    .remoteAddress(address).connect();
            try {
                future.syncUninterruptibly();
            } catch (Throwable e) {
                throw new TarantoolClientException(e);
            }
            if (!future.isSuccess()) {
                throw new TarantoolClientException(
                        "Failed to connect to the Tarantool server in %d milliseconds", config.getConnectTimeout());
            }
            try {
                conn = connectionFuture
                        .thenApply(ch -> new TarantoolConnectionImpl(config, versionHolder, futureManager, ch))
                        .thenApplyAsync(c -> {
                            try {
                                c.metadata().refresh().get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
                                return c;
                            } catch (Throwable e) {
                                throw new CompletionException(e);
                            }
                        })
                        .get(config.getConnectTimeout(), TimeUnit.MILLISECONDS);
                connections.put(address, conn);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new TarantoolClientException(e);
            }
        }
        return conn;
    }

    @Override
    public TarantoolClientConfig getConfig() {
        return config;
    }

    @Override
    public void close() throws Exception {
        try {
            for (TarantoolConnection conn: connections.values()) {
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
