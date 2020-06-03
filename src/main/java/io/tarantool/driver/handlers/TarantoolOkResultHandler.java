package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolRequestMetadata;
import io.tarantool.driver.protocol.TarantoolOkResult;

import java.util.concurrent.CompletableFuture;

public class TarantoolOkResultHandler extends SimpleChannelInboundHandler<TarantoolOkResult> {
    private RequestFutureManager futureManager;

    public TarantoolOkResultHandler(RequestFutureManager futureManager) {
        super();
        this.futureManager = futureManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolOkResult result) throws Exception {
        TarantoolRequestMetadata requestMeta = futureManager.getRequest(result.getSyncId());
        if (requestMeta != null) {
            CompletableFuture<?> requestFuture = requestMeta.getFeature();
            if (!requestFuture.isDone()) {
                requestFuture.complete(requestMeta.getMapper().fromValue(result.getData()));
            }
        }
    }
}
