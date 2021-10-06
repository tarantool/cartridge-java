package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;

/**
 * Tarantool client configurator implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientConfiguratorImpl implements TarantoolClientConfigurator {

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
    public TarantoolClientConfigurator withProxyMethodMapping() {
        return withProxyMethodMapping(UnaryOperator.identity());
    }

    @Override
    public TarantoolClientConfigurator withProxyMethodMapping(
            UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        this.mappingConfig = builder.apply(ProxyOperationsMappingConfig.builder()).build();
        return this;
    }

    @Override
    public TarantoolClientConfigurator withRetryingByNumberOfAttempts(int numberOfAttempts) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, UnaryOperator.identity());
    }

    @Override
    public TarantoolClientConfigurator withRetryingByNumberOfAttempts(
            int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
            .AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> TarantoolClientConfigurator withRetryingByNumberOfAttempts(
            int numberOfAttempts, T exceptionsCheck,
            UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                .builder(numberOfAttempts, exceptionsCheck)).build());
    }

    @Override
    public TarantoolClientConfigurator withRetryingIndefinitely(UnaryOperator<TarantoolRequestRetryPolicies
            .InfiniteRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingIndefinitely(retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> TarantoolClientConfigurator withRetryingIndefinitely(
            T callback, UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.builder(callback))
                .build());
    }

    @Override
    public TarantoolClientConfigurator withRetrying(RequestRetryPolicyFactory factory) {
        this.retryPolicyFactory = factory;
        return this;
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
}
