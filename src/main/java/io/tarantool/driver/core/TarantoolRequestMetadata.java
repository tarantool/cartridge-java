package io.tarantool.driver.core;

import io.tarantool.driver.mappers.MessagePackValueMapper;

import java.util.concurrent.CompletableFuture;

/**
 * Intermediate request metadata holder
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestMetadata {
    private final CompletableFuture<?> feature;
    private final MessagePackValueMapper mapper;

    protected TarantoolRequestMetadata(CompletableFuture<?> feature, MessagePackValueMapper mapper) {
        this.feature = feature;
        this.mapper = mapper;
    }

    public CompletableFuture<?> getFuture() {
        return feature;
    }

    public MessagePackValueMapper getMapper() {
        return mapper;
    }
}
