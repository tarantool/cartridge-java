package io.tarantool.driver.metadata;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolMetadataRequestException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Populates metadata from results of a call to proxy API function in Tarantool instance. The function result is
 * expected to have the format which is returned by DDL module.
 * See <a href="https://github.com/tarantool/ddl#input-data-format">
 * https://github.com/tarantool/ddl#input-data-format</a>
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public class ProxyTarantoolMetadata extends AbstractTarantoolMetadata {

    private final String metadataFunctionName;
    private final TarantoolClient client;
    private final ProxyTarantoolSpaceMetadataConverter metadataConverter;

    public ProxyTarantoolMetadata(String metadataFunctionName,
                                  TarantoolClient client) {
        this.metadataFunctionName = metadataFunctionName;
        this.client = client;
        this.metadataConverter = new ProxyTarantoolSpaceMetadataConverter(client.getConfig().getMessagePackMapper());
    }

    @Override
    public CompletableFuture<Void> populateMetadata() throws TarantoolClientException {

        CompletableFuture<TarantoolResult<ProxyTarantoolSpaceMetadataContainer>> callResult = client.call(
                metadataFunctionName,
                metadataConverter);

        return callResult.thenAccept(result -> {
            spaceMetadata.clear();
            spaceMetadataById.clear();
            indexMetadata.clear();
            indexMetadataBySpaceId.clear();
            result.forEach(container -> {
                spaceMetadata.putAll(container.getSpaceMetadata());
                indexMetadata.putAll(container.getIndexMetadata());
                container.getSpaceMetadata().forEach((spaceName, spaceMetadata) -> {
                    spaceMetadataById.put(spaceMetadata.getSpaceId(), spaceMetadata);
                    Map<String, TarantoolIndexMetadata> indexesForSpace =
                            indexMetadata.get(spaceMetadata.getSpaceName());
                    if (indexesForSpace != null) {
                        indexMetadataBySpaceId.put(spaceMetadata.getSpaceId(), indexesForSpace);
                    }
                });
            });
        }).exceptionally(ex -> {
            if (ex.getCause() != null && ex.getCause() instanceof TarantoolClientException) {
                throw new TarantoolMetadataRequestException(metadataFunctionName, ex);
            } else if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new CompletionException(ex);
            }
        });
    }
}
