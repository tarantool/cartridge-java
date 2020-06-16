package io.tarantool.driver.core;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.auth.ChapSha1TarantoolAuthenticator;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.codecs.MessagePackFrameCodec;
import io.tarantool.driver.handlers.TarantoolAuthenticationHandler;
import io.tarantool.driver.handlers.TarantoolErrorResultHandler;
import io.tarantool.driver.handlers.TarantoolOkResultHandler;
import io.tarantool.driver.handlers.TarantoolRequestHandler;
import io.tarantool.driver.handlers.TarantoolResponseHandler;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;

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
    private RequestFutureManager futureManager;

    public TarantoolChannelInitializer(TarantoolClientConfig config, TarantoolVersionHolder versionHolder,
                                       RequestFutureManager futureManager) {
        this.config = config;
        this.versionHolder = versionHolder;
        this.futureManager = futureManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        ctx.pipeline().addFirst("TarantoolAuthenticationHandler", new TarantoolAuthenticationHandler<>(
                versionHolder,
                (SimpleTarantoolCredentials) config.getCredentials(),
                new ChapSha1TarantoolAuthenticator()));
        ctx.fireChannelActive();
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        socketChannel.pipeline()
                .addLast("MessagePackFrameCodec", new MessagePackFrameCodec(
                        DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper()))
                // outbound
                .addLast("TarantoolRequestHandler", new TarantoolRequestHandler(futureManager))
                // inbound
                .addLast("TarantoolResponseHandler", new TarantoolResponseHandler())
                .addLast("TarantoolErrorResultHandler", new TarantoolErrorResultHandler(futureManager))
                .addLast("TarantoolOkResultHandler", new TarantoolOkResultHandler(futureManager));
    }
}
