package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadataResult;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.api.metadata.TarantoolMetadataProvider;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadataResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;
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
    private final VSpaceToTarantoolSpaceMetadataConverter spaceMetadataMapper;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;

    /**
     * Basic constructor
     *
     * @param client            configured client instance
     * @param messagePackMapper MessagePack mapper configured for the caller client
     */
    public SpacesMetadataProvider(
        TarantoolCallOperations client,
        MessagePackMapper messagePackMapper) {
        this.client = client;
        this.spaceMetadataMapper = VSpaceToTarantoolSpaceMetadataConverter.getInstance();
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

    private <T> CompletableFuture<TarantoolResult<T>> select(
        String selectCmd,
        ValueConverter<ArrayValue, T> resultConverter,
        Class<? extends SingleValueCallResult<TarantoolResult<T>>> resultClass)
        throws TarantoolClientException {
        CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper =
            client.getResultMapperFactoryFactory().<T>singleValueTarantoolResultMapperFactory()
                .withSingleValueArrayTarantoolResultConverter(resultConverter, resultClass);
        return client.call(selectCmd, resultMapper);
    }
}
