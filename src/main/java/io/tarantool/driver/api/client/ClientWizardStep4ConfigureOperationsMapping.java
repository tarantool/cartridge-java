package io.tarantool.driver.api.client;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.UnaryOperator;

public class ClientWizardStep4ConfigureOperationsMapping {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private ProxyOperationsMappingConfig proxyOperationsMappingConfig;

    public ClientWizardStep4ConfigureOperationsMapping(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    public ClientWizardStep5ConfigureRetryPolicy withDefaultCrudMethods() {
        return new ClientWizardStep5ConfigureRetryPolicy(this.tarantoolClient);
    }

    public ClientWizardStep5ConfigureRetryPolicy withMappedCrudMethods(
            UnaryOperator<ProxyOperationsMappingConfig.Builder> mappingConfigBuilderFunction) {
        this.proxyOperationsMappingConfig = mappingConfigBuilderFunction
                .apply(ProxyOperationsMappingConfig.builder())
                .build();

        return new ClientWizardStep5ConfigureRetryPolicy(makeProxyClient());
    }

    private ProxyTarantoolTupleClient makeProxyClient() {
        return new ProxyTarantoolTupleClient(this.tarantoolClient, this.proxyOperationsMappingConfig);
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return tarantoolClient;
    }
}
