package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolRequestMetadata;
import io.tarantool.driver.exceptions.TarantoolDecoderException;
import io.tarantool.driver.exceptions.errors.TarantoolErrors;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import io.tarantool.driver.protocol.TarantoolOkResult;
import io.tarantool.driver.protocol.TarantoolResponse;

import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool server response handler. Dispatches incoming message either to an error or a normal result handler.
 *
 * @author Alexey Kuzin
 */
public class TarantoolResponseHandler extends SimpleChannelInboundHandler<TarantoolResponse> {

    private final Logger log = LoggerFactory.getLogger(TarantoolResponseHandler.class);
    private final TarantoolErrors.TarantoolBoxErrorFactory boxErrorFactory
        = new TarantoolErrors.TarantoolBoxErrorFactory();
    private final RequestFutureManager futureManager;

    public TarantoolResponseHandler(RequestFutureManager futureManager) {
        super();
        this.futureManager = futureManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolResponse tarantoolResponse) throws Exception {
        TarantoolRequestMetadata requestMeta = futureManager.getRequest(tarantoolResponse.getSyncId());
        if (requestMeta != null) {
            CompletableFuture<Value> requestFuture = requestMeta.getFuture();
            if (!requestFuture.isDone()) {
                switch (tarantoolResponse.getResponseType()) {
                    case IPROTO_NOT_OK:
                        TarantoolErrorResult errorResult = new TarantoolErrorResult(tarantoolResponse.getSyncId(),
                            tarantoolResponse.getResponseCode(), tarantoolResponse.getBody().getData());
                        requestFuture.completeExceptionally(boxErrorFactory.create(errorResult));
                        break;
                    case IPROTO_OK:
                        try {
                            TarantoolOkResult okResult = new TarantoolOkResult(tarantoolResponse.getSyncId(),
                                tarantoolResponse.getBody().getData());
                            requestFuture.complete(okResult.getData());
                        } catch (Throwable e) {
                            requestFuture.completeExceptionally(e);
                        }
                }
            }
        } else {
            log.info("Request {} is not registered in this client instance", tarantoolResponse.getSyncId());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof DecoderException && cause.getCause() instanceof TarantoolDecoderException) {
            TarantoolDecoderException ex = (TarantoolDecoderException) cause.getCause();
            TarantoolRequestMetadata requestMeta = futureManager.getRequest(ex.getHeader().getSync());
            if (requestMeta != null) {
                CompletableFuture<Value> requestFuture = requestMeta.getFuture();
                if (!requestFuture.isDone()) {
                    requestFuture.completeExceptionally(cause);
                    return;
                }
            }
        }
        super.exceptionCaught(ctx, cause);
    }
}
