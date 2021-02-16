package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

/**
 * {@link ProxyTarantoolSpace} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class ProxyTarantoolTupleSpace extends ProxyTarantoolSpace<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    private final TarantoolClientConfig config;
    private final TarantoolCallOperations client;

    /**
     * Basic constructor
     *
     * @param config Tarantool client config
     * @param client configured Tarantool client
     * @param mappingConfig proxy operation mapping config
     * @param spaceMetadata current space metadata
     * @param metadataOperations metadata operations
     */
    public ProxyTarantoolTupleSpace(TarantoolClientConfig config,
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
    protected
    CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    tupleResultMapper() {
        return client.getResultMapperFactoryFactory().defaultTupleSingleResultMapperFactory()
                .withDefaultTupleValueConverter(config.getMessagePackMapper(), getMetadata());
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", getMetadata().getSpaceName());
    }
}
