package io.tarantool.driver.core;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.exceptions.TarantoolClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manages instantiation and connection procedures for Tarantool server connections
 *
 * @author Alexey Kuzin
 */
public class TarantoolConnectionFactory {

    private final TarantoolClientConfig config;
    private final Bootstrap bootstrap;
    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    /**
     * Basic constructor.
     * @param config Tarantool client config
     * @param bootstrap prepared Netty's bootstrap
     */
    public TarantoolConnectionFactory(TarantoolClientConfig config, Bootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
    }

    /**
     * Create single connection and return connection future
     * @param serverAddress Tarantool server address to connect
     * @return connection future
     */
    public CompletableFuture<TarantoolConnection> singleConnection(InetSocketAddress serverAddress) {
        CompletableFuture<Channel> connectionFuture = new CompletableFuture<>();
        RequestFutureManager requestManager = new RequestFutureManager(config);
        TarantoolVersionHolder versionHolder = new TarantoolVersionHolder();
        ChannelFuture future = bootstrap.clone()
                .handler(new TarantoolChannelInitializer(config, requestManager, versionHolder, connectionFuture))
                .remoteAddress(serverAddress).connect();
        future.addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                connectionFuture.completeExceptionally(new TarantoolClientException(
                        String.format("Failed to connect to the Tarantool server at %s", serverAddress), f.cause()));
            }
        });
        return connectionFuture
                .thenApply(ch -> new TarantoolConnectionImpl(requestManager, versionHolder, ch))
                .handle((v, ex) -> {
                    if (ex != null) {
                        logger.error("Connection failed: ", ex);
                        return null;
                    }
                    return v;
                });
    }

    /**
     * Create several connections and return their futures
     * @param serverAddress Tarantool server address to connect
     * @param connections number of connections to create
     * @return a collection with specified number of connection futures
     */
    public Collection<CompletableFuture<TarantoolConnection>> multiConnection(InetSocketAddress serverAddress,
                                                                              int connections) {
        return Stream.generate(() -> serverAddress)
                .map(this::singleConnection)
                .limit(connections)
                .collect(Collectors.toList());
    }
}
