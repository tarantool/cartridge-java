package io.tarantool.driver.core;

import java.util.concurrent.CompletableFuture;

import org.msgpack.value.Value;

/**
 * Intermediate request metadata holder
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestMetadata {
    private final CompletableFuture<Value> future;

    protected TarantoolRequestMetadata(CompletableFuture<Value> future) {
        this.future = future;
    }

    public CompletableFuture<Value> getFuture() {
        return future;
    }
}
