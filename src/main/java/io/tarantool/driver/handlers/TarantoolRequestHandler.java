package io.tarantool.driver.handlers;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.protocol.TarantoolRequest;

/**
 * Performs registration of requests and pushes them forward. Should stay first in the channel pipeline
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestHandler extends ChannelOutboundHandlerAdapter {
    private RequestFutureManager futureManager;

    public TarantoolRequestHandler(RequestFutureManager futureManager) {
        this.futureManager = futureManager;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        TarantoolRequest request = (TarantoolRequest) msg;
        ctx.write(request).addListener((ChannelFutureListener) channelFuture -> {
            if (!channelFuture.isSuccess()) {
                futureManager.getRequest(request.getHeader().getSync()).getFeature()
                        .completeExceptionally(new TarantoolClientException(channelFuture.cause()));
                ctx.channel().close(); //TODO return channel to pool
            }
        });
        ctx.write(msg);
    }
}
