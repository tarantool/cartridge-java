package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.TarantoolServerException;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.protocol.TarantoolErrorResult;

import java.util.concurrent.CompletableFuture;

/**
 * Handles error responses from Tarantool server and marks the corresponding requests as failed
 *
 * @author Alexey Kuzin
 */
public class TarantoolErrorResultHandler extends SimpleChannelInboundHandler<TarantoolErrorResult> {

    private RequestFutureManager futureManager;

    public TarantoolErrorResultHandler(RequestFutureManager futureManager) {
        super();
        this.futureManager = futureManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, TarantoolErrorResult errorResult) {
        CompletableFuture requestFuture = futureManager.getRequest(errorResult.getSyncId()).getFeature();
        if (requestFuture != null && !requestFuture.isDone()) {
            //TODO different error types based on codes (factory)
            requestFuture.completeExceptionally(
                new TarantoolServerException(errorResult.getErrorCode(), errorResult.getErrorMessage()));
        }
    }
}
