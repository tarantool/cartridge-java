package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.AddressProvider;
import io.tarantool.driver.cluster.AddressProviderWithClusterDiscovery;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
    private ConcurrentHashMap<InetSocketAddress, TarantoolConnection> connections;
    private AddressProvider addressProvider;
    private AtomicBoolean isDiscoveryTaskActive = new AtomicBoolean(false);

    /**
     * Create a client. Default credentials will be used.
     * @see InetSocketAddress
     */
    public StandaloneTarantoolClient() {
        this(TarantoolClientConfig.builder().build());
    }

    /**
     * Create a client using provided credentials information.
     * @param credentials Tarantool user credentials holder
     * @see TarantoolCredentials
     */
    public StandaloneTarantoolClient(TarantoolCredentials credentials) {
       this(TarantoolClientConfig.builder()
               .withCredentials(credentials)
               .build());
    }

    /**
     * Create a client.
     * @param config the client configuration
     * @see TarantoolClientConfig
     */
    public StandaloneTarantoolClient(TarantoolClientConfig config) {
        this.config = config;
        this.addressProvider = config.getAddressProvider();
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
     * Connects to the Tarantool server and, if necessary, starts a discovery task.
     * @return connected client
     * @throws TarantoolClientException when connection or request for metadata are failed
     */
    @Override
    public TarantoolConnection connect() throws TarantoolClientException {
        if (config.getClusterDiscoveryEndpoint() != null &&
                addressProvider instanceof AddressProviderWithClusterDiscovery &&
                isDiscoveryTaskActive.compareAndSet(false, true)) {
            ((AddressProviderWithClusterDiscovery)addressProvider).runClusterDiscovery(config, this);
        }

        ServerAddress serverAddress = addressProvider.getNext();
        return connect(serverAddress.getSocketAddress());
    }

    private TarantoolConnection connect(InetSocketAddress address) throws TarantoolClientException {
        TarantoolConnection conn = connections.get(address);
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
