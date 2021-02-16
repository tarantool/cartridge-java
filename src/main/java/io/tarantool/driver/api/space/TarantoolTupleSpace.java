package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ResultMapperFactoryFactory;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.operations.TupleOperations;

/**
 * Tarantool space API implementation using default tuple implementation
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleSpace extends StandaloneTarantoolSpace<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    private final TarantoolClientConfig config;
    private final ResultMapperFactoryFactory mapperFactoryFactory;

    /**
     * Basic constructor
     *
     * @param config client config
     * @param connectionManager Tarantool server connection manager
     * @param spaceMetadata metadata for this space
     * @param metadataOperations metadata operations implementation
     * @param mapperFactoryFactory factory for factories producing result mappers
     */
    public TarantoolTupleSpace(TarantoolClientConfig config,
                               TarantoolConnectionManager connectionManager,
                               TarantoolSpaceMetadata spaceMetadata,
                               TarantoolMetadataOperations metadataOperations,
                               ResultMapperFactoryFactory mapperFactoryFactory) {
        super(config, connectionManager, metadataOperations, spaceMetadata);
        this.config = config;
        this.mapperFactoryFactory = mapperFactoryFactory;
    }

    @Override
    protected TupleOperations makeOperationsFromTuple(TarantoolTuple tuple) {
        return TupleOperations.fromTarantoolTuple(tuple);
    }

    @Override
    protected MessagePackValueMapper tupleResultMapper() {
        return mapperFactoryFactory.defaultTupleResultMapperFactory()
                .withDefaultTupleValueConverter(config.getMessagePackMapper(), getMetadata());
    }

    @Override
    public String toString() {
        return String.format("TarantoolSpace %s [%d]", getMetadata().getSpaceName(), getMetadata().getSpaceId());
    }
}
