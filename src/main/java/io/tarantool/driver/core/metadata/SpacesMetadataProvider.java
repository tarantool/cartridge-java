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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
    private final
        CallResultMapper<TarantoolResult<TarantoolSpaceMetadata>,
            SingleValueCallResult<TarantoolResult<TarantoolSpaceMetadata>>>
        spaceMetadataResultMapper;
    private final
        Supplier<CallResultMapper<TarantoolResult<TarantoolSpaceMetadata>,
            SingleValueCallResult<TarantoolResult<TarantoolSpaceMetadata>>>>
        spaceMetadataResultMapperSupplier;
    private final TarantoolIndexMetadataConverter indexMetadataMapper;
    private final
        CallResultMapper<TarantoolResult<TarantoolIndexMetadata>,
            SingleValueCallResult<TarantoolResult<TarantoolIndexMetadata>>>
        indexMetadataResultMapper;
    private final
        Supplier<CallResultMapper<TarantoolResult<TarantoolIndexMetadata>,
            SingleValueCallResult<TarantoolResult<TarantoolIndexMetadata>>>>
        indexMetadataResultMapperSupplier;

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
        this.spaceMetadataResultMapper = client
            .getResultMapperFactoryFactory().<TarantoolSpaceMetadata>singleValueTarantoolResultMapperFactory()
            .withSingleValueArrayTarantoolResultConverter(spaceMetadataMapper, TarantoolSpaceMetadataResult.class);
        this.spaceMetadataResultMapperSupplier = () -> spaceMetadataResultMapper;
        this.indexMetadataMapper = new TarantoolIndexMetadataConverter(messagePackMapper);
        this.indexMetadataResultMapper = client
            .getResultMapperFactoryFactory().<TarantoolIndexMetadata>singleValueTarantoolResultMapperFactory()
            .withSingleValueArrayTarantoolResultConverter(indexMetadataMapper, TarantoolIndexMetadataResult.class);
        this.indexMetadataResultMapperSupplier = () -> indexMetadataResultMapper;
    }

    @Override
    public CompletableFuture<TarantoolMetadataContainer> getMetadata() throws TarantoolClientException {

        CompletableFuture<TarantoolResult<TarantoolSpaceMetadata>> spaces =
            select(VSPACE_SELECT_CMD, spaceMetadataResultMapperSupplier, TarantoolSpaceMetadata.class);
        CompletableFuture<TarantoolResult<TarantoolIndexMetadata>> indexes =
            select(VINDEX_SELECT_CMD, indexMetadataResultMapperSupplier, TarantoolIndexMetadata.class);

        return spaces.thenCombine(indexes, SpacesTarantoolMetadataContainer::new);
    }

    private <T> CompletableFuture<TarantoolResult<T>> select(
        String selectCmd,
        Supplier<CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>> resultMapperSupplier,
        Class<T> resultClass)
        throws TarantoolClientException {
        return client.call(selectCmd, resultMapperSupplier);
    }
}
