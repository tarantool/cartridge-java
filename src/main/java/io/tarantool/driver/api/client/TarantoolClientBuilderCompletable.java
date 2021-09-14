package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

public interface TarantoolClientBuilderCompletable {

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
