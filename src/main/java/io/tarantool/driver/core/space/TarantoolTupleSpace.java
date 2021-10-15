package io.tarantool.driver.core.space;

import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.OffsetCursor;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.connection.TarantoolConnectionManager;
import io.tarantool.driver.mappers.MessagePackValueMapper;

/**
 * {@link TarantoolSpace} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleSpace extends
        TarantoolSpace<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    private final TarantoolCallOperations client;
    private final TarantoolClientConfig config;

    /**
     * Basic constructor
     *
     * @param client client that provides connection to tarantool server
     * @param config client config
     * @param connectionManager Tarantool server connection manager
     * @param spaceMetadata metadata for this space
     * @param metadataOperations metadata operations implementation
     */    public TarantoolTupleSpace(TarantoolCallOperations client,
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

    @Override
    public String toString() {
        return String.format("TarantoolSpace %s [%d]", getMetadata().getSpaceName(), getMetadata().getSpaceId());
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions, int batchSize) {
        return new OffsetCursor<>(this, conditions, batchSize);
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions) {
        return cursor(conditions, config.getCursorBatchSize());
    }
}
