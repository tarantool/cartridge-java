package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

/**
 * {@link StandaloneTarantoolSpace} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class StandaloneTarantoolTupleSpace extends
        StandaloneTarantoolSpace<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    private final TarantoolCallOperations client;
    private final TarantoolClientConfig config;

    public StandaloneTarantoolTupleSpace(TarantoolCallOperations client,
                                         TarantoolClientConfig config,
                                         TarantoolConnectionManager connectionManager,
                                         TarantoolMetadataOperations metadataOperations,
                                         TarantoolSpaceMetadata spaceMetadata) {
        super(config, connectionManager, metadataOperations, spaceMetadata);
        this.client = client;
        this.config = config;
    }

    @Override
    protected TupleOperations makeOperationsFromTuple(TarantoolTuple tuple) {
        return TupleOperations.fromTarantoolTuple(tuple);
    }

    @Override
    protected MessagePackValueMapper tupleResultMapper() {
        return client.getResultMapperFactoryFactory().defaultTupleResultMapperFactory()
                .withDefaultTupleValueConverter(config.getMessagePackMapper(), getMetadata());
    }
}
