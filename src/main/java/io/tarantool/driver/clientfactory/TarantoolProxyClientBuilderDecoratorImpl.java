package io.tarantool.driver.clientfactory;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.clientbuilder.TarantoolProxyClientBuilder;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

public class TarantoolProxyClientBuilderDecoratorImpl
        extends AbstractTarantoolClientBuilderDecorator<ProxyTarantoolTupleClient, TarantoolProxyClientBuilder>
        implements TarantoolProxyClientBuilderDecorator {

    private final TarantoolProxyClientBuilder proxyBuilder;

    public TarantoolProxyClientBuilderDecoratorImpl() {
        this.proxyBuilder = ProxyTarantoolTupleClient.builder()
                .withDecoratedClient(super.getBuilder().build());
    }

    public TarantoolProxyClientBuilderDecoratorImpl(TarantoolProxyClientBuilder proxyBuilder) {
        this.proxyBuilder = proxyBuilder;
    }

    @Override
    public ProxyTarantoolTupleClient build() {

        return this.proxyBuilder.build();
    }

    @Override
    public TarantoolProxyClientBuilderDecorator withMappingConfig(ProxyOperationsMappingConfig mappingConfig) {
        return new TarantoolProxyClientBuilderDecoratorImpl(proxyBuilder.withMappingConfig(mappingConfig));
    }

    @Override
    public TarantoolProxyClientBuilderDecorator withDecoratedClient(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> client) {
        return new TarantoolProxyClientBuilderDecoratorImpl(proxyBuilder.withDecoratedClient(client));
    }
}
