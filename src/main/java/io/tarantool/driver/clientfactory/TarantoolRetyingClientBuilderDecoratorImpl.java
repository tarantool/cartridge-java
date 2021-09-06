package io.tarantool.driver.clientfactory;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.clientbuilder.TarantoolRetryingClientBuilder;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;

import java.util.concurrent.Executor;

public class TarantoolRetyingClientBuilderDecoratorImpl
        extends AbstractTarantoolClientBuilderDecorator<RetryingTarantoolTupleClient, TarantoolRetryingClientBuilder>
        implements TarantoolRetryingClientBuilderDecorator {

    private final TarantoolRetryingClientBuilder retryingBuilder;

    public TarantoolRetyingClientBuilderDecoratorImpl(TarantoolRetryingClientBuilder retryingBuilder) {
        this.retryingBuilder = retryingBuilder;
    }

    public TarantoolRetyingClientBuilderDecoratorImpl() {
        this.retryingBuilder = RetryingTarantoolTupleClient.builder()
                .withDecoratedClient(super.getBuilder().build());
    }

    @Override
    public RetryingTarantoolTupleClient build() {
        return this.retryingBuilder.build();
    }

    @Override
    public TarantoolRetryingClientBuilderDecorator withRetryPolicyFactory(
            RequestRetryPolicyFactory retryPolicyFactory) {
        return new TarantoolRetyingClientBuilderDecoratorImpl(this.retryingBuilder
                .withRetryPolicyFactory(retryPolicyFactory));
    }

    @Override
    public TarantoolRetryingClientBuilderDecorator withExecutor(Executor executor) {
        return new TarantoolRetyingClientBuilderDecoratorImpl(this.retryingBuilder.withExecutor(executor));
    }

    @Override
    public TarantoolRetryingClientBuilderDecorator withDecoratedClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return new TarantoolRetyingClientBuilderDecoratorImpl(this.retryingBuilder.withDecoratedClient(client));
    }
}
