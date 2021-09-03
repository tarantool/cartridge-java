package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;

import java.util.concurrent.Executor;

public interface TarantoolRetryingClientBuilder
        extends TarantoolDecoratedClientBuilder
        <RetryingTarantoolTupleClient, TarantoolRetryingClientBuilder, ProxyTarantoolTupleClient> {

    TarantoolRetryingClientBuilder INSTANCE = new TarantoolRetryingClientBuilderImpl();

    TarantoolRetryingClientBuilder withRetryPolicyFactory(RequestRetryPolicyFactory retryPolicyFactory);

    TarantoolRetryingClientBuilder withExecutor(Executor executor);
}
