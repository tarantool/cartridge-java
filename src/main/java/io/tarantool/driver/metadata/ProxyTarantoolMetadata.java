package io.tarantool.driver.metadata;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Populates metadata from results of a call to proxy API function in Tarantool instance. The function result is
 * expected to have the format which is returned by DDL module.
 * See <a href="https://github.com/tarantool/ddl#input-data-format ">
 * https://github.com/tarantool/ddl#input-data-format</a>
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public class ProxyTarantoolMetadata extends AbstractTarantoolMetadata {

    private final String getMetadataFunctionName;
    private final TarantoolClient client;
    private final ProxyTarantoolSpaceMetadataConverter metadataConverter;

    public ProxyTarantoolMetadata(String getMetadataFunctionName,
                                  TarantoolClient client) {
        this.getMetadataFunctionName = getMetadataFunctionName;
        this.client = client;
        this.metadataConverter = new ProxyTarantoolSpaceMetadataConverter(client.getConfig().getMessagePackMapper());
    }

    @Override
    public CompletableFuture<Void> populateMetadata() throws TarantoolClientException {

        CompletableFuture<TarantoolResult<ProxyTarantoolSpaceMetadataContainer>> callResult = client.call(
                getMetadataFunctionName,
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
                    Map<String, TarantoolIndexMetadata> indexesForSpace = indexMetadata.get(spaceMetadata.getSpaceName());
                    if (indexesForSpace != null) {
                        indexMetadataBySpaceId.put(spaceMetadata.getSpaceId(), indexesForSpace);
                    }
                });
            });
        });
    }
}
