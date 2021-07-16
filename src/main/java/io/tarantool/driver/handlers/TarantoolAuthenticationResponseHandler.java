package io.tarantool.driver.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.exceptions.errors.TarantoolErrors;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import io.tarantool.driver.protocol.TarantoolResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool server authentication response handler. Completes connection or makes exception out of Tarantool
 * server error
 *
 * @author Alexey Kuzin
 */
public class TarantoolAuthenticationResponseHandler extends SimpleChannelInboundHandler<TarantoolResponse> {

    private final CompletableFuture<Channel> connectionFuture;
    private final TarantoolErrors.TarantoolBoxErrorFactory boxErrorFactory
            = new TarantoolErrors.TarantoolBoxErrorFactory();
    public TarantoolAuthenticationResponseHandler(CompletableFuture<Channel> connectionFuture) {
        super();
        this.connectionFuture = connectionFuture;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolResponse tarantoolResponse) throws Exception {
        if (!connectionFuture.isDone()) {
            switch (tarantoolResponse.getResponseType()) {
                case IPROTO_NOT_OK:
                    TarantoolErrorResult errorResult = new TarantoolErrorResult(tarantoolResponse.getSyncId(),
                            tarantoolResponse.getResponseCode(), tarantoolResponse.getBody().getData());
                    connectionFuture.completeExceptionally(boxErrorFactory.create(errorResult));
                    break;
                case IPROTO_OK:
                    connectionFuture.complete(ctx.channel());
            }
        }
        ctx.pipeline().remove(this); // authorize once per channel
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        connectionFuture.completeExceptionally(cause);
    }
}
