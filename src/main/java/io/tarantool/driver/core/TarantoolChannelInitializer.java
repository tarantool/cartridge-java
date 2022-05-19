package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.auth.ChapSha1TarantoolAuthenticator;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.codecs.MessagePackFrameDecoder;
import io.tarantool.driver.codecs.MessagePackFrameEncoder;
import io.tarantool.driver.handlers.TarantoolAuthenticationHandler;
import io.tarantool.driver.handlers.TarantoolAuthenticationResponseHandler;
import io.tarantool.driver.handlers.TarantoolRequestHandler;
import io.tarantool.driver.handlers.TarantoolResponseHandler;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The main channel pipeline initializer.
 * <p>
 * - Adds authentication handler which accepts the Tarantool server greeting and sets up the pipeline when channel
 * is connect to the server;
 * - Sets up the necessary handlers and codecs.
 *
 * @author Alexey Kuzin
 */
public class TarantoolChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final TarantoolClientConfig config;
    private final TarantoolVersionHolder versionHolder;
    private final CompletableFuture<Channel> connectionFuture;
    private final RequestFutureManager futureManager;

    public TarantoolChannelInitializer(TarantoolClientConfig config,
                                       RequestFutureManager futureManager,
                                       TarantoolVersionHolder versionHolder,
                                       CompletableFuture<Channel> connectionFuture) {
        this.config = config;
        this.versionHolder = versionHolder;
        this.connectionFuture = connectionFuture;
        this.futureManager = futureManager;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) {
        final ChannelPipeline pipeline = socketChannel.pipeline();

        final SslContext sslContext = config.getSslContext();
        if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(socketChannel.alloc()));
        }

        // greeting and authentication (will be removed after successful authentication)
        pipeline.addLast("TarantoolAuthenticationHandler",
                new TarantoolAuthenticationHandler<>(
                        connectionFuture,
                        versionHolder,
                        (SimpleTarantoolCredentials) config.getCredentials(),
                        new ChapSha1TarantoolAuthenticator()))
                // frame encoder and decoder
                .addLast("MessagePackFrameDecoder", new MessagePackFrameDecoder())
                .addLast("MessagePackFrameEncoder", new MessagePackFrameEncoder(
                        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()))
                // outbound
                .addLast("TarantoolRequestHandler", new TarantoolRequestHandler(futureManager))
                // inbound auth response handler
                .addLast("TarantoolAuthenticationResponseHandler", new TarantoolAuthenticationResponseHandler(
                        connectionFuture))
                // inbound
                .addLast("TarantoolResponseHandler", new TarantoolResponseHandler(futureManager));
    }
}
