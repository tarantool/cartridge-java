package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder interface.
 * It can be used for creating client to tarantool with different parameters.
 */
public interface TarantoolClientBuilder {

    /**
     * Specify addresses to tarantool instances
     *
     * @param address list of addresses to tarantool instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(TarantoolServerAddress... address);

    /**
     * Specify user credentials
     *
     * @param tarantoolCredentials credentials for instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withCredentials(TarantoolCredentials tarantoolCredentials);

    /**
     * Specify connection selection strategy
     *
     * @param tarantoolConnectionSelectionStrategyType type of connection selection strategy
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType);

    /**
     * Specify lambda builder for proxying function names
     *
     * @param builder lambda with builder of proxy mapping
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withProxyMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    /**
     * Specify number of attempts for requests retrying
     *
     * @param numberOfAttempts number of attempts for requests retrying
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRetryAttemptsInAmount(int numberOfAttempts);

    /**
     * Specify delay in ms between requests
     *
     * @param delayMs delay in ms between requests
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRetryDelay(long delayMs);

    /**
     * Specify timeout between retrying requests
     *
     * @param requestTimeoutMs timeout between retrying requests
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestTimeout(long requestTimeoutMs);

    /**
     * Specify handler of exception for requests retrying
     *
     * @param callback handler of exception for requests retrying
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withExceptionCallback(Function<Throwable, Boolean> callback);

    /**
     * Build the basic Tarantool client
     *
     * @return this instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
