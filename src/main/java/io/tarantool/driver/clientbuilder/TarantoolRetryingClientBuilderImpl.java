package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;

import java.util.concurrent.Executor;

class TarantoolRetryingClientBuilderImpl
        extends AbstractTarantoolDecoratedClientBuilder
        <RetryingTarantoolTupleClient, TarantoolRetryingClientBuilder, ProxyTarantoolTupleClient>
        implements TarantoolRetryingClientBuilder {

    private RequestRetryPolicyFactory retryPolicyFactory;
    private Executor executor;

    public TarantoolRetryingClientBuilderImpl() {
        initBuilder();
    }

    @Override
    public RetryingTarantoolTupleClient build() {
        if (executor == null) {
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
