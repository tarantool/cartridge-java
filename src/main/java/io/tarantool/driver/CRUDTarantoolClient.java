package io.tarantool.driver;

import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.proxy.CRUDOperationsMappingConfig;
import io.tarantool.driver.proxy.CRUDTarantoolMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;

import java.util.Collection;

/**
 * @author Sergey Volgin
 */
public class CRUDTarantoolClient extends ProxyTarantoolClient {

    private final CRUDOperationsMappingConfig operationsMappingConfig;
    private final CRUDTarantoolMetadata metadataOperations;

    public CRUDTarantoolClient(CRUDOperationsMappingConfig operationsMappingConfig,
                               TarantoolClientConfig config,
                               Collection<TarantoolServerAddress> addresses) {
        this(operationsMappingConfig, config, () -> addresses, ParallelRoundRobinStrategyFactory.INSTANCE);
    }

    public CRUDTarantoolClient(CRUDOperationsMappingConfig operationsMappingConfig,
                               TarantoolClientConfig config,
                               TarantoolClusterAddressProvider addressProvider,
                               ConnectionSelectionStrategyFactory selectStrategyFactory) {
        this.client = new ClusterTarantoolClient(config, addressProvider, selectStrategyFactory);
        this.operationsMappingConfig = operationsMappingConfig;
        this.metadataOperations = new CRUDTarantoolMetadata(
                operationsMappingConfig.getGetSchemaFunctionName(),
                this.client
        );
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        throw new TarantoolClientException("Proxy client doesn't support work with space by ID");
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) {
        return new ProxyTarantoolSpace(this, spaceName, this.metadata(), this.operationsMappingConfig);
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        return metadataOperations;
    }
}
