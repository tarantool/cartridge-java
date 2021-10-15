package io.tarantool.driver.api;

import io.tarantool.driver.api.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.api.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client configurator interface.
 * <p>
 * Provides a single entry point for configuring all types of Tarantool clients.
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientConfigurator<SELF extends TarantoolClientConfigurator<SELF>> {

    /**
     * Specify using the default CRUD proxy operations mapping configuration. For using the default operations mapping,
     * make sure the tarantool/crud module is installed as a dependency and enabled in your application.
     *
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    SELF withProxyMethodMapping();

    /**
     * Configure a custom operations proxy configuration.
     * This configuration allows specifying custom Lua function names callable on the Tarantool server,
     * for replacing the default space operations with these functions calls. This allows, for example,
     * replacing the default schema retrieving method or writing a custom "insert" implementation.
     *
     * @param builder builder provider instance, e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    SELF withProxyMethodMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    /**
     * Specify the number of retry attempts for each request.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    SELF withRetryingByNumberOfAttempts(int numberOfAttempts);

    /**
     * Configure the attempts bound request retry policy.
     * Only the requests that failed with known network exceptions will be retried by default.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @param policy           builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *                         e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    SELF withRetryingByNumberOfAttempts(
            int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
            .AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy);

    /**
     * Configure the attempts bound request retry policy.
     *
     * @param numberOfAttempts the number of retry attempts for each request
     * @param exceptionsCheck  function checking whether the given exception may be retried
     * @param policy           builder provider for {@link TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy},
     *                         e.g. a lambda function taking the builder
     * @param <T>              callback type for exceptions check
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    <T extends Function<Throwable, Boolean>> SELF withRetryingByNumberOfAttempts(
            int numberOfAttempts, T exceptionsCheck,
            UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy);

    /**
     * Configure the infinite request retry policy.
     * Only the requests that failed with known network exceptions will be retried by default.
     *
     * @param policy builder provider for {@link TarantoolRequestRetryPolicies.InfiniteRetryPolicy},
     *               e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    SELF withRetryingIndefinitely(
            UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder
                    <Function<Throwable, Boolean>>> policy);

    /**
     * Configure the infinite request retry policy.
     *
     * @param policy   builder provider for {@link TarantoolRequestRetryPolicies.InfiniteRetryPolicy},
     *                 e.g. a lambda function taking the builder
     * @param callback function checking whether the given exception may be retried
     * @param <T>      callback type for exceptions check
     * @return this instance of builder {@link TarantoolClientConfigurator}
     */
    <T extends Function<Throwable, Boolean>> SELF withRetryingIndefinitely(
            T callback,
            UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy);

    /**
     * Specify a custom request retry policy factory. A request retry policy encapsulates an algorithm of checking
     * if a particular failed request needs to be repeated. The built-in request retry policies include customizable
     * policy variants with a bounded or unbounded number of retries.
     *
     * @param factory {@link RequestRetryPolicyFactory}
     * @return this instance of builder {@link TarantoolClientConfigurator}
     * @see TarantoolRequestRetryPolicies
     */
    SELF withRetrying(RequestRetryPolicyFactory factory);

    /**
     * Build the configured Tarantool client instance. Call this when you have specified all necessary settings.
     *
     * @return instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
