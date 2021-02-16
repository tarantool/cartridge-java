package io.tarantool.driver.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.ResultMapperFactoryFactory;
import io.tarantool.driver.mappers.SingleValueTarantoolResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;

import java.util.concurrent.CompletableFuture;

/**
 * Provides spaces and index metadata via requests to the system spaces in the Tarantool server instance
 *
 * @author Alexey Kuzin
 */
public class SpacesMetadataProvider implements TarantoolMetadataProvider {

    static final String VSPACE_SELECT_CMD = "box.space._vspace:select"; // System space with all space descriptions
    static final String VINDEX_SELECT_CMD = "box.space._vindex:select"; // System space with all index descriptions

    private final TarantoolCallOperations client;
    private final TarantoolSpaceMetadataConverter spaceMetadataMapper;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;

    /**
     * Basic constructor
     *
     * @param client configured client instance
     * @param messagePackMapper MessagePack mapper configured for the caller client
     */
    public SpacesMetadataProvider(TarantoolCallOperations client,
                                  MessagePackMapper messagePackMapper) {
        this.client = client;
        this.spaceMetadataMapper = new TarantoolSpaceMetadataConverter(messagePackMapper);
        this.indexMetadataMapper = new TarantoolIndexMetadataConverter(messagePackMapper);
    }

    @Override
    public CompletableFuture<TarantoolMetadataContainer> getMetadata() throws TarantoolClientException {

        CompletableFuture<TarantoolResult<TarantoolSpaceMetadata>> spaces =
                select(VSPACE_SELECT_CMD, spaceMetadataMapper, TarantoolSpaceMetadataResult.class);
        CompletableFuture<TarantoolResult<TarantoolIndexMetadata>> indexes =
                select(VINDEX_SELECT_CMD, indexMetadataMapper, TarantoolIndexMetadataResult.class);

        return spaces.thenCombine(indexes, SpacesTarantoolMetadataContainer::new);
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<TarantoolResult<T>> select(
            String selectCmd,
            ValueConverter<ArrayValue, T> resultConverter,
            Class<? extends SingleValueCallResult<TarantoolResult<T>>> resultClass)
            throws TarantoolClientException {
        CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper =
                ((SingleValueTarantoolResultMapperFactory<T>)
                        client.getResultMapperFactoryFactory().singleValueTarantoolResultMapperFactory(resultClass))
                        .withTarantoolResultConverter(resultConverter, resultClass);
        return client.call(selectCmd, resultMapper);
    }
}
