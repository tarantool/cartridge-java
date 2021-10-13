package io.tarantool.driver.api.metadata;

import java.util.concurrent.CompletableFuture;

/**
 * Generic interface for different space and index metadata providers
 *
 * @author Alexey Kuzin
 */
public interface TarantoolMetadataProvider {
    /**
     * Retrieve the metadata from an external source
     *
     * @return future resulting in a container with the space and index metadata
     */
    CompletableFuture<TarantoolMetadataContainer> getMetadata();
}
