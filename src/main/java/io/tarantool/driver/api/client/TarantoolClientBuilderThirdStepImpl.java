package io.tarantool.driver.api.client;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.UnaryOperator;

public class TarantoolClientBuilderThirdStepImpl implements TarantoolClientBuilderThirdStep {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private ProxyOperationsMappingConfig proxyOperationsMappingConfig;

    public TarantoolClientBuilderThirdStepImpl(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    @Override
    public TarantoolClientBuilderFourthStep withDefaultCrudMethods() {
        return new TarantoolClientBuilderFourthStepImpl(this.tarantoolClient);
    }

    @Override
    public TarantoolClientBuilderFourthStep withMappedCrudMethods(
            UnaryOperator<ProxyOperationsMappingConfig.Builder> mappingConfigBuilderFunction) {
        this.proxyOperationsMappingConfig = mappingConfigBuilderFunction
                .apply(ProxyOperationsMappingConfig.builder())
                .build();

        return new TarantoolClientBuilderFourthStepImpl(makeProxyClient());
    }

    private ProxyTarantoolTupleClient makeProxyClient() {
        return new ProxyTarantoolTupleClient(this.tarantoolClient, this.proxyOperationsMappingConfig);
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return tarantoolClient;
    }
}
