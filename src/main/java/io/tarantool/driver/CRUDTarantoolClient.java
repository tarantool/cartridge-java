package io.tarantool.driver;

import io.tarantool.driver.api.space.ProxyTarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.proxy.CRUDOperationsMapping;
import io.tarantool.driver.proxy.CRUDTarantoolMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;

import java.util.Collection;

/**
 * @author Sergey Volgin
 */
public class CRUDTarantoolClient extends ProxyTarantoolClient implements CRUDOperationsMapping {

    private final CRUDTarantoolMetadata metadataOperations;

    public CRUDTarantoolClient(TarantoolClientConfig config,
                               Collection<TarantoolServerAddress> addresses) {
        this(config, () -> addresses, ParallelRoundRobinStrategyFactory.INSTANCE);
    }

    public CRUDTarantoolClient(TarantoolClientConfig config,
                               TarantoolClusterAddressProvider addressProvider,
                               ConnectionSelectionStrategyFactory selectStrategyFactory) {
        this.client = new ClusterTarantoolClient(config, addressProvider, selectStrategyFactory);
        this.metadataOperations = new CRUDTarantoolMetadata(
                this.getGetSchemaFunctionName(),
                this.client
        );
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        throw new TarantoolClientException("Proxy client doesn't support work with space by ID");
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) {
        return new ProxyTarantoolSpace(this, spaceName, this.metadata());
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        return metadataOperations;
    }
}
