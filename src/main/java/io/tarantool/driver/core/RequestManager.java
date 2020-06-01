package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.concurrent.CompletableFuture;

/**
 * Responsible for submitting request into the driver infrastructure
 *
 * @author Alexey Kuzin
 */
public class RequestManager {
    private final Channel channel;
    private final RequestFutureManager futureManager;

    public RequestManager(Channel channel, RequestFutureManager futureManager) {
        this.channel = channel;
        this.futureManager = futureManager;
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request) {
        CompletableFuture<T> requestFuture = futureManager.addRequestFuture(request.getHeader().getSync());
        channel.writeAndFlush(request);
        return requestFuture;
    }
}
