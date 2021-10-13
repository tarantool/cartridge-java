package io.tarantool.driver.core.space;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.StartAfterCursor;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.mappers.CallResultMapper;

/**
 * {@link ProxyTarantoolSpaceOperations} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class ProxyTarantoolTupleSpaceOperations
        extends ProxyTarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    private final TarantoolClientConfig config;
    private final TarantoolCallOperations client;

    /**
     * Basic constructor
     *
     * @param config             Tarantool client config
     * @param client             configured Tarantool client
     * @param mappingConfig      proxy operation mapping config
     * @param spaceMetadata      current space metadata
     * @param metadataOperations metadata operations
     */
    public ProxyTarantoolTupleSpaceOperations(TarantoolClientConfig config,
                                              TarantoolCallOperations client,
                                              ProxyOperationsMappingConfig mappingConfig,
                                              TarantoolSpaceMetadata spaceMetadata,
                                              TarantoolMetadataOperations metadataOperations) {
        super(config, client, mappingConfig, metadataOperations, spaceMetadata);
        this.config = config;
        this.client = client;
    }

    @Override
    protected TupleOperations makeOperationsFromTuple(TarantoolTuple tuple) {
        return TupleOperations.fromTarantoolTuple(tuple);
    }

    @Override
    protected CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    tupleResultMapper() {
        return client.getResultMapperFactoryFactory().defaultTupleSingleResultMapperFactory()
                .withDefaultTupleValueConverter(config.getMessagePackMapper(), getMetadata());
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", getMetadata().getSpaceName());
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions, int batchSize) {
        return new StartAfterCursor<>(this, conditions, batchSize, config.getMessagePackMapper());
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions) {
        return cursor(conditions, config.getCursorBatchSize());
    }
}
