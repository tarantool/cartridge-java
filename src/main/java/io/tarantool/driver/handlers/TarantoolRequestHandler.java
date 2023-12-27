package io.tarantool.driver.handlers;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolRequestMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.TarantoolRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs registration of requests and pushes them forward. Should stay first in the channel pipeline
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestHandler extends ChannelOutboundHandlerAdapter {
    private final Logger log = LoggerFactory.getLogger(TarantoolRequestHandler.class);
    private final RequestFutureManager futureManager;

    public TarantoolRequestHandler(RequestFutureManager futureManager) {
        this.futureManager = futureManager;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        TarantoolRequest request = (TarantoolRequest) msg;
        ctx.writeAndFlush(request, promise).addListener((ChannelFutureListener) channelFuture -> {
            if (!channelFuture.isSuccess()) {
                TarantoolRequestMetadata requestMeta = futureManager.getRequest(request.getHeader().getSync());
                // The request metadata may has been deleted already after timeout
                if (requestMeta != null) {
                    requestMeta.getFuture()
                        .completeExceptionally(new TarantoolClientException(channelFuture.cause()));
                } else {
                    log.info(
                        "Received an error for {} but it is already timed out: {}", request, channelFuture.cause());
                }
            }
        });
    }
}
