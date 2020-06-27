package io.tarantool.driver.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.TarantoolServerException;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolRequestMetadata;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import io.tarantool.driver.protocol.TarantoolOkResult;
import io.tarantool.driver.protocol.TarantoolResponse;
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
    private RequestFutureManager futureManager;

    public TarantoolResponseHandler(RequestFutureManager futureManager) {
        super();
        this.futureManager = futureManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TarantoolResponse tarantoolResponse) throws Exception {
        TarantoolRequestMetadata requestMeta = futureManager.getRequest(tarantoolResponse.getSyncId());
        if (requestMeta != null) {
            CompletableFuture<?> requestFuture = requestMeta.getFeature();
            if (!requestFuture.isDone()) {
                switch (tarantoolResponse.getResponseType()) {
                    case IPROTO_NOT_OK:
                        TarantoolErrorResult errorResult = new TarantoolErrorResult(tarantoolResponse.getSyncId(),
                                tarantoolResponse.getResponseCode(), tarantoolResponse.getBody().getData());
                        //TODO different error types based on codes (factory)
                        requestFuture.completeExceptionally(
                            new TarantoolServerException(errorResult.getErrorCode(), errorResult.getErrorMessage()));
                        break;
                    case IPROTO_OK:
                        try {
                            TarantoolOkResult okResult = new TarantoolOkResult(tarantoolResponse.getSyncId(),
                                    tarantoolResponse.getBody().getData());
                            requestFuture.complete(requestMeta.getMapper().fromValue(okResult.getData()));
                        } catch (Throwable e) {
                            requestFuture.completeExceptionally(e);
                        }
                }
            }
        } else {
            log.info("Request {} is not registered in this client instance", tarantoolResponse.getSyncId());
        }
    }
}
