package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.utils.Assert;

class TarantoolProxyClientBuilderImpl
        extends AbstractTarantoolDecoratedClientBuilder
        <ProxyTarantoolTupleClient, TarantoolProxyClientBuilder,
                TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
        implements TarantoolProxyClientBuilder {

    private ProxyOperationsMappingConfig mappingConfig;

    @Override
    public ProxyTarantoolTupleClient build() {
        Assert.notNull(super.getDecoratedClient(), "Decorated client must not be null! " +
                "Please invoke withDecoratedClient()");

        if (mappingConfig == null) {
            return new ProxyTarantoolTupleClient(super.getDecoratedClient());
        }
        return new ProxyTarantoolTupleClient(new ClusterTarantoolTupleClient(), mappingConfig);
    }

    @Override
    public TarantoolProxyClientBuilder withMappingConfig(ProxyOperationsMappingConfig mappingConfig) {
        this.mappingConfig = mappingConfig;
        return this;
    }

    public TarantoolProxyClientBuilderImpl() {
        initBuilder();
    }

    @Override
    protected void initBuilder() {
        super.instance = this;
    }
}
