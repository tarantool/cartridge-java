package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfigurator;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.utils.Assert;

import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;

/**
 * Tarantool client configurator implementation.
 *
 * @author Oleg Kuznetsov
 * @author Artyom Dubinin
 */
public class TarantoolClientConfiguratorImpl<SELF extends TarantoolClientConfigurator<SELF>>
    implements TarantoolClientConfigurator<SELF> {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    protected RequestRetryPolicyFactory retryPolicyFactory;
    protected ProxyOperationsMappingConfig mappingConfig;
    protected Executor executor;

    public TarantoolClientConfiguratorImpl(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        this.client = client;
    }

    protected TarantoolClientConfiguratorImpl() {
        this.client = null;
    }

    @Override
    public SELF withProxyMethodMapping() {
        return withProxyMethodMapping(UnaryOperator.identity());
    }

    @Override
    public SELF withProxyMethodMapping(
        UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        this.mappingConfig = builder.apply(ProxyOperationsMappingConfig.builder()).build();
        return getSelf();
    }

    @Override
    public SELF withRetryingByNumberOfAttempts(int numberOfAttempts) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, UnaryOperator.identity());
    }

    @Override
    public SELF withRetryingByNumberOfAttempts(
        int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
        .AttemptsBoundRetryPolicyFactory.Builder<Predicate<Throwable>>> policy) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, retryNetworkErrors(), policy);
    }

    @Override
    public SELF withRetryingByNumberOfAttempts(
        int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
        .AttemptsBoundRetryPolicyFactory.Builder<Predicate<Throwable>>> policy, Executor executor) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, retryNetworkErrors(), policy, executor);
    }

    @Override
    public <T extends Predicate<Throwable>> SELF withRetryingByNumberOfAttempts(
        int numberOfAttempts, T exceptionsCheck,
        UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
            .builder(numberOfAttempts, exceptionsCheck)).build());
    }

    @Override
    public <T extends Predicate<Throwable>> SELF withRetryingByNumberOfAttempts(
        int numberOfAttempts, T exceptionsCheck,
        UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy,
        Executor executor) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
            .builder(numberOfAttempts, exceptionsCheck)).build(), executor);
    }

    @Override
    public SELF withRetryingIndefinitely(
        UnaryOperator<TarantoolRequestRetryPolicies
            .InfiniteRetryPolicyFactory.Builder<Predicate<Throwable>>> policy, Executor executor) {
        return withRetryingIndefinitely(retryNetworkErrors(), policy, executor);
    }

    @Override
    public SELF withRetryingIndefinitely(
        UnaryOperator<TarantoolRequestRetryPolicies
            .InfiniteRetryPolicyFactory.Builder<Predicate<Throwable>>> policy) {
        return withRetryingIndefinitely(retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Predicate<Throwable>> SELF withRetryingIndefinitely(
        T callback, UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.builder(callback))
            .build());
    }

    @Override
    public <T extends Predicate<Throwable>> SELF withRetryingIndefinitely(
        T callback, UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy,
        Executor executor) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.builder(callback))
            .build(), executor);
    }

    @Override
    public SELF withRetrying(RequestRetryPolicyFactory factory) {
        this.retryPolicyFactory = factory;
        return getSelf();
    }

    @Override
    public SELF withRetrying(RequestRetryPolicyFactory factory, Executor executor) {
        this.retryPolicyFactory = factory;
        this.executor = executor;
        return getSelf();
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return decorate(this.client);
    }

    /**
     * Decorates provided client by user specified parameters.
     *
     * @param client Tarantool client for decorating
     * @return decorated client or the same client
     * if parameters for decorating in {@link TarantoolClientConfigurator} have not been provided
     */
    protected TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    decorate(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        Assert.notNull(client, "Tarantool client must not be null!");

        if (this.mappingConfig != null) {
            client = new ProxyTarantoolTupleClient(client, this.mappingConfig);
        }
        if (this.retryPolicyFactory != null) {
            if (this.executor != null) {
                client = new RetryingTarantoolTupleClient(client, this.retryPolicyFactory, executor);
            } else {
                client = new RetryingTarantoolTupleClient(client, this.retryPolicyFactory);
            }
        }

        return client;
    }

    @SuppressWarnings("unchecked")
    private SELF getSelf() {
        return (SELF) this;
    }
}
