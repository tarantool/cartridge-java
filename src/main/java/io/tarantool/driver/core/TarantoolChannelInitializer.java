package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.auth.ChapSha1TarantoolAuthenticator;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.codecs.MessagePackFrameEncoder;
import io.tarantool.driver.codecs.MessagePackFrameDecoder;
import io.tarantool.driver.handlers.TarantoolAuthenticationHandler;
import io.tarantool.driver.handlers.TarantoolAuthenticationResponseHandler;
import io.tarantool.driver.handlers.TarantoolRequestHandler;
import io.tarantool.driver.handlers.TarantoolResponseHandler;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The main channel pipeline initializer.
 *
 *  - Adds authentication handler which accepts the Tarantool server greeting and sets up the pipeline when channel
 *  is connect to the server;
 *  - Sets up the necessary handlers and codecs.
 *
 * @author Alexey Kuzin
 */
public class TarantoolChannelInitializer extends ChannelInitializer<SocketChannel> {

    private TarantoolClientConfig config;
    private TarantoolVersionHolder versionHolder;
    private CompletableFuture<Channel> connectionFuture;
    private RequestFutureManager futureManager;

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
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline()
                // greeting and authentication (will be removed after successful authentication)
                .addLast("TarantoolAuthenticationHandler", new TarantoolAuthenticationHandler<>(
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
