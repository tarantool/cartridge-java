package io.tarantool.driver;

import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.ProxyTarantoolTupleSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

/**
 * {@link ProxyTarantoolClient} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class ProxyTarantoolTupleClient extends ProxyTarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> {

    /**
     * Basic constructor. Uses default values for proxy operations mapping.
     *
     * @param decoratedClient configured Tarantool client
     */
    public ProxyTarantoolTupleClient(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedClient) {
        this(decoratedClient, ProxyOperationsMappingConfig.builder().build());
    }

    /**
     * Basic constructor
     *
     * @param decoratedClient configured Tarantool client
     * @param mappingConfig   config for proxy operations mapping
     */
    public ProxyTarantoolTupleClient(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedClient,
                                     ProxyOperationsMappingConfig mappingConfig) {
        super(decoratedClient, mappingConfig);
    }

    @Override
    protected TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> spaceOperations(
            TarantoolClientConfig config,
            TarantoolCallOperations client,
            ProxyOperationsMappingConfig mappingConfig,
            TarantoolMetadataOperations metadata,
            TarantoolSpaceMetadata spaceMetadata) {
        return new ProxyTarantoolTupleSpace(config, client, mappingConfig, spaceMetadata, metadata);
    }
}
