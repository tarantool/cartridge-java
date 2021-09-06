package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;

import java.util.concurrent.Executor;

public interface TarantoolRetryingClientBuilder
        extends TarantoolDecoratedClientBuilder
        <RetryingTarantoolTupleClient, TarantoolRetryingClientBuilder, TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>> {

    TarantoolRetryingClientBuilder INSTANCE = new TarantoolRetryingClientBuilderImpl();

    TarantoolRetryingClientBuilder withRetryPolicyFactory(RequestRetryPolicyFactory retryPolicyFactory);

    TarantoolRetryingClientBuilder withExecutor(Executor executor);
}
