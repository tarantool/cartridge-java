package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client configurator interface.
 * <p>
 * Provides a single entry point for configuring all types of Tarantool clients.
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientConfigurator {

    /**
     * Specify using proxy methods.
     *
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withProxyMethodMapping();

    /**
     * Specify a builder provider for operations proxy configuration.
     * This configuration allows specifying custom Lua function names callable on the Tarantool server,
     * for replacing the default space operations with these functions calls. This allows, for example,
     * replacing the default schema retrieving method or writing a custom "insert" implementation.
     *
     * @param builder builder provider instance, e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withProxyMethodMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    /**
     * Specify retry attempts bound.
     * By default all requests with network exceptions will be retry.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withRetryingByNumberOfAttempts(int numberOfAttempts);

    /**
     * Specify provider for attempts bound retry policy.
     * By default all requests with network exceptions will be retry.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @param policy           builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *                         e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withRetryingByNumberOfAttempts(
            int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
            .AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy);

    /**
     * Specify provider for attempts bound retry policy.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @param policy           builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *                         e.g. a lambda function taking the builder
     * @param <T>              callback type for exceptions check
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    <T extends Function<Throwable, Boolean>> TarantoolClientConfigurator withRetryingByNumberOfAttempts(
            int numberOfAttempts, T exceptionsCheck,
            UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy);

    /**
     * Specify provider for infinite retry policy.
     * By default all requests with network exceptions will be retry.
     *
     * @param policy builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *               e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withRetryingIndefinitely(
            UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder
                    <Function<Throwable, Boolean>>> policy);

    /**
     * Specify provider for infinite retry policy.
     *
     * @param policy builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *               e.g. a lambda function taking the builder
     * @param <T>    callback type for exceptions check
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    <T extends Function<Throwable, Boolean>> TarantoolClientConfigurator withRetryingIndefinitely(
            T callback,
            UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy);

    /**
     * Specify request retry policy factory.
     *
     * @param factory {@link RequestRetryPolicyFactory}
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    TarantoolClientConfigurator withRetrying(RequestRetryPolicyFactory factory);

    /**
     * Build the configured Tarantool client instance. Call this when you have specified all necessary settings.
     *
     * @return instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
