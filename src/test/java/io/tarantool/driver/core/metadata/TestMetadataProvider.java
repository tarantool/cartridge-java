package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;

import java.util.concurrent.CompletableFuture;

/**
 * @author Alexey Kuzin
 */
public class TestMetadataProvider implements TarantoolMetadataProvider {

    public TestMetadataProvider() {
    }

    @Override
    public CompletableFuture<TarantoolMetadataContainer> getMetadata() {
        return CompletableFuture.completedFuture(new TestMetadataContainer());
    }
}
