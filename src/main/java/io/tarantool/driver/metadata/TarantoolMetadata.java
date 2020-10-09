package io.tarantool.driver.metadata;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolSimpleResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolSelectRequest;
import org.msgpack.value.ArrayValue;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Populates metadata from system spaces on a standalone Tarantool instance
 *
 * @author Alexey Kuzin
 */
public class TarantoolMetadata extends AbstractTarantoolMetadata {

    private static final int VSPACE_SPACE_ID = 281; // System space with all space descriptions (_vspace)
    private static final int VINDEX_SPACE_ID = 289; // System space with all index descriptions (_vindex)

    private final TarantoolSpaceMetadataConverter spaceMetadataMapper;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;
    private final TarantoolClientConfig config;
    private final TarantoolConnectionManager connectionManager;
    private final TarantoolSimpleResultMapperFactory mapperFactory;

    /**
     * Basic constructor.
     *
     * @param config client configuration
     * @param connectionManager configured {@link TarantoolConnectionManager} instance
     */
    public TarantoolMetadata(TarantoolClientConfig config, TarantoolConnectionManager connectionManager) {
        super();
        this.spaceMetadataMapper = new TarantoolSpaceMetadataConverter(config.getMessagePackMapper());
        this.indexMetadataMapper = new TarantoolIndexMetadataConverter(config.getMessagePackMapper());
        this.config = config;
        this.connectionManager = connectionManager;
        this.mapperFactory = new TarantoolSimpleResultMapperFactory(config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<Void> populateMetadata() throws TarantoolClientException {

        CompletableFuture<TarantoolResult<TarantoolSpaceMetadata>> spaces =
                select(VSPACE_SPACE_ID, spaceMetadataMapper);
        CompletableFuture<TarantoolResult<TarantoolIndexMetadata>> indexes =
                select(VINDEX_SPACE_ID, indexMetadataMapper);

        return spaces.thenAcceptBoth(indexes, (spacesCollection, indexesCollection) -> {
            spaceMetadata.clear(); // clear the metadata only after the result fetching is successful
            spaceMetadataById.clear();
            indexMetadata.clear();
            indexMetadataBySpaceId.clear();

            spacesCollection.forEach(meta -> {
                spaceMetadata.put(meta.getSpaceName(), meta);
                spaceMetadataById.put(meta.getSpaceId(), meta);
            });

            indexesCollection.forEach(meta -> {
                String spaceName = spaceMetadataById.get(meta.getSpaceId()).getSpaceName();
                indexMetadata.putIfAbsent(spaceName, new HashMap<>());
                indexMetadata.get(spaceName).put(meta.getIndexName(), meta);
                indexMetadataBySpaceId.putIfAbsent(meta.getSpaceId(), new HashMap<>());
                indexMetadataBySpaceId.get(meta.getSpaceId()).put(meta.getIndexName(), meta);
            });
        });
    }

    private <T> CompletableFuture<TarantoolResult<T>> select(int spaceId,  ValueConverter<ArrayValue, T> resultMapper)
            throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY)
                    .withIteratorType(TarantoolIteratorType.ITER_ALL);
            TarantoolSelectOptions options = new TarantoolSelectOptions.Builder().build();

            TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
                    .withSpaceId(spaceId)
                    .withIndexId(indexQuery.getIndexId())
                    .withIteratorType(indexQuery.getIteratorType())
                    .withKeyValues(indexQuery.getKeyValues())
                    .withLimit(options.getLimit())
                    .withOffset(options.getOffset())
                    .build(config.getMessagePackMapper());

            return connectionManager.getConnection().sendRequest(request, mapperFactory.withConverter(resultMapper));
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }
}
