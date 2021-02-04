package io.tarantool.driver.metadata;

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
