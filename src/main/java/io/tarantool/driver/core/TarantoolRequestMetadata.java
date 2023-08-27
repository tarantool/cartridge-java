package io.tarantool.driver.core;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.msgpack.value.Value;

import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.TarantoolRequestSignature;

/**
 * Intermediate request metadata holder
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestMetadata {
    private final TarantoolRequest request;
    private final CompletableFuture<Value> future;

    protected TarantoolRequestMetadata(TarantoolRequest request, CompletableFuture<Value> requestFuture) {
        this.request = request;
        this.future = requestFuture;
    }

    public CompletableFuture<Value> getFuture() {
        return future;
    }

    public Long getRequestId() {
        return request.getHeader().getSync();
    }

    @Override
    public String toString() {
        return request.toString();
    }
}
