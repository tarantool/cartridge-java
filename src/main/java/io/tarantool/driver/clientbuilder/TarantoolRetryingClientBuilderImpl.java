package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.Executor;

class TarantoolRetryingClientBuilderImpl
        extends AbstractTarantoolDecoratedClientBuilder
        <RetryingTarantoolTupleClient, TarantoolRetryingClientBuilder,
                TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>>
        implements TarantoolRetryingClientBuilder {

    private RequestRetryPolicyFactory retryPolicyFactory;
    private Executor executor;

    public TarantoolRetryingClientBuilderImpl() {
        initBuilder();
    }

    @Override
    public RetryingTarantoolTupleClient build() {
        Assert.notNull(this.retryPolicyFactory,
                "RetryPolicyFactory must not be null! Invoke withRetryPolicyFactory()");
        Assert.notNull(super.getDecoratedClient(), "Decorated client must not be null! " +
                "Please invoke withDecoratedClient()");

        if (this.executor == null) {
            return new RetryingTarantoolTupleClient(super.getDecoratedClient(), this.retryPolicyFactory);
        }

        return new RetryingTarantoolTupleClient(
                super.getDecoratedClient(), this.retryPolicyFactory, this.executor);
    }

    @Override
    protected void initBuilder() {
        super.instance = this;
    }

    @Override
    public TarantoolRetryingClientBuilderImpl withRetryPolicyFactory(RequestRetryPolicyFactory retryPolicyFactory) {
        this.retryPolicyFactory = retryPolicyFactory;
        return this;
    }

    @Override
    public TarantoolRetryingClientBuilder withExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }
}
