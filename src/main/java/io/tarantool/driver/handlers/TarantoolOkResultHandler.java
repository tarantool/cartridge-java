package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolOkResult;

import java.util.concurrent.CompletableFuture;

public class TarantoolOkResultHandler extends SimpleChannelInboundHandler<TarantoolOkResult> {
    private RequestFutureManager futureManager;
    private MessagePackObjectMapper mapper;

    public TarantoolOkResultHandler(RequestFutureManager futureManager, MessagePackObjectMapper mapper) {
        super();
        this.futureManager = futureManager;
        this.mapper = mapper;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolOkResult result) throws Exception {
        CompletableFuture<?> requestFuture = futureManager.getRequestFuture(result.getSyncId());
        if (requestFuture != null && !requestFuture.isDone()) {
            requestFuture.complete(mapper.fromValue(result.getData()));
        }
    }
}
