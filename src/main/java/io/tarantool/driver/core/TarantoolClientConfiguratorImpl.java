package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfigurator;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;

/**
 * Tarantool client configurator implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientConfiguratorImpl<SELF extends TarantoolClientConfigurator<SELF>>
        implements TarantoolClientConfigurator<SELF> {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    protected RequestRetryPolicyFactory retryPolicyFactory;
    protected ProxyOperationsMappingConfig mappingConfig;

    public TarantoolClientConfiguratorImpl(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        this.client = client;
    }

    protected TarantoolClientConfiguratorImpl() {
        this.client = new ClusterTarantoolTupleClient();
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
            .AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> SELF withRetryingByNumberOfAttempts(
            int numberOfAttempts, T exceptionsCheck,
            UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                .builder(numberOfAttempts, exceptionsCheck)).build());
    }

    @Override
    public SELF withRetryingIndefinitely(UnaryOperator<TarantoolRequestRetryPolicies
            .InfiniteRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingIndefinitely(retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> SELF withRetryingIndefinitely(
            T callback, UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.builder(callback))
                .build());
    }

    @Override
    public SELF withRetrying(RequestRetryPolicyFactory factory) {
        this.retryPolicyFactory = factory;
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

        if (this.mappingConfig != null) {
            client = new ProxyTarantoolTupleClient(client, this.mappingConfig);
        }
        if (this.retryPolicyFactory != null) {
            client = new RetryingTarantoolTupleClient(client, this.retryPolicyFactory);
        }

        return client;
    }

    @SuppressWarnings("unchecked")
    private SELF getSelf() {
        return (SELF) this;
    }
}
