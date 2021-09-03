package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

public interface TarantoolProxyClientBuilder
        extends TarantoolDecoratedClientBuilder
        <ProxyTarantoolTupleClient, TarantoolProxyClientBuilder,
                TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>> {

    TarantoolProxyClientBuilder INSTANCE = new TarantoolProxyClientBuilderImpl();

    TarantoolProxyClientBuilder withMappingConfig(ProxyOperationsMappingConfig mappingConfig);
}
