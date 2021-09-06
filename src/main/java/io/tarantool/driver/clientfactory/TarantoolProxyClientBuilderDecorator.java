package io.tarantool.driver.clientfactory;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

public interface TarantoolProxyClientBuilderDecorator
        extends TarantoolClientBuilderDecorator<ProxyTarantoolTupleClient> {

    TarantoolProxyClientBuilderDecorator withDecoratedClient(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> client);
}
