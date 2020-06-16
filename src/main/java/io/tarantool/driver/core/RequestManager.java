package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.tarantool.driver.mappers.MessagePackValueMapper;
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

    /**
     * Basic constructor.
     * @param channel Netty channel
     * @param futureManager keeps track of sent requests
     */
    public RequestManager(Channel channel, RequestFutureManager futureManager) {
        this.channel = channel;
        this.futureManager = futureManager;
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<T> submitRequest(TarantoolRequest request, MessagePackValueMapper mapper) {
        CompletableFuture<T> requestFuture = futureManager.addRequest(request.getHeader().getSync(), mapper);
        channel.writeAndFlush(request);
        return requestFuture;
    }
}
