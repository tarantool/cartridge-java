package io.tarantool.driver.clientfactory;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;

public interface TarantoolRetryingClientBuilderDecorator
        extends TarantoolClientBuilderDecorator<RetryingTarantoolTupleClient> {

    TarantoolRetryingClientBuilderDecorator withDecoratedClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client);
}
