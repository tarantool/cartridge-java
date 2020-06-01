package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import io.tarantool.driver.protocol.TarantoolOkResult;
import io.tarantool.driver.protocol.TarantoolResponse;

/**
 * Basic Tarantool server response handler. Dispatches incoming message either to an error or a normal result handler.
 *
 * @author Alexey Kuzin
 */
public class TarantoolResponseHandler extends SimpleChannelInboundHandler<TarantoolResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolResponse tarantoolResponse) throws Exception {
        switch (tarantoolResponse.getResponseType()) {
            case IPROTO_NOT_OK:
                ctx.write(new TarantoolErrorResult(tarantoolResponse.getSyncId(),
                        tarantoolResponse.getResponseCode(), tarantoolResponse.getBody().getData()));
                break;
            case IPROTO_OK:
                ctx.write(new TarantoolOkResult(tarantoolResponse.getSyncId(), tarantoolResponse.getBody().getData()));
        }
    }
}
