package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;

public class TarantoolClientBuilderSecondStepImpl implements TarantoolClientBuilderSecondStep {

    private final SimpleTarantoolCredentials credentials;
    private final TarantoolClusterAddressProvider addressProvider;

    public TarantoolClientBuilderSecondStepImpl(SimpleTarantoolCredentials credentials,
                                                TarantoolClusterAddressProvider addressProvider) {
        this.credentials = credentials;
        this.addressProvider = addressProvider;
    }


    @Override
    public TarantoolClientBuilderThirdStep withDefaultConnectionSelectionStrategy() {
        return withConnectionSelectionStrategy(ConnectionSelectionStrategyType.defaultType());
    }

    @Override
    public TarantoolClientBuilderThirdStep withConnectionSelectionStrategy(
            ConnectionSelectionStrategyType selectionStrategyType) {
        return new TarantoolClientBuilderThirdStepImpl(makeBaseClient(selectionStrategyType));
    }

    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> makeBaseClient(
            ConnectionSelectionStrategyType type) {
        return new ClusterTarantoolTupleClient(
                TarantoolClientConfig.builder()
                        .withCredentials(this.credentials)
                        .withConnectionSelectionStrategyFactory(type.value())
                        .build(), this.addressProvider
        );
    }
}
