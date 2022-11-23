package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolClientBuilderImpl;
import io.tarantool.driver.core.TarantoolClientConfiguratorImpl;

/**
 * Tarantool client factory interface.
 * <p>
 * It provides a builder interface helping to create the basic Tarantool client types while hiding the internal client
 * implementation details from the user.
 *
 * <p>
 * Here are some examples of using this client factory:
 * <pre>
 * <code>
 *
 * // Create a client instance with default settings. This client can connect to a local Tarantool process listening the
 * default port 3301 (do not forget enabling it by executing this command in console: `box.cfg{ listen = 3301 }`).
 * TarantoolClientFactory.createClient().build();
 *
 * // Create a client instance for a single server with custom credentials
 * TarantoolClientFactory.createClient()
 *             .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *             .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *             .build();
 *
 * // Create a client instance with custom proxy operations mapping
 * TarantoolClientFactory.createClient()
 *             .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *             .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *             .withProxyMethodMapping(builder -&gt; builder.withDeleteFunctionName("custom_delete"))
 *             .build();
 *
 * // Create a client instance with request retry policy
 * TarantoolClientFactory.createClient()
 *            .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *            .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *            .withRetryingByNumberOfAttempts(5, throwable -&gt; throwable.getMessage().equals("Some error"),
 *                   policy -&gt; policy.withDelay(500)
 *                           .withRequestTimeout(1000)
 *            .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
 *            .build();
 *
 * // Create a client instance with proxy operations mapping and request retry policy
 * TarantoolClientFactory.createClient()
 *           .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *           .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *           .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
 *           .withProxyMethodMapping(builder -&gt; builder.withReplaceFunctionName("custom_replace")
 *                   .withTruncateFunctionName("create"))
 *           .withRetryingByNumberOfAttempts(5, throwable -&gt; throwable.getMessage().equals("Some error"),
 *                   policy -&gt; policy.withDelay(500)
 *                           .withRequestTimeout(1000)
 *           ).build();
 *
 * // Create a client instance with request retry policy from an existing configured client
 * TarantoolClientFactory.configureClient(client)
 *           .withRetryingByNumberOfAttempts(5, throwable -&gt; throwable.getMessage().equals("Some error"),
 *                   policy -&gt; policy.withDelay(500)
 *                           .withRequestTimeout(1000)
 *           ).build();
 *
 * // Create a client instance with proxy operations mapping from an existing configured client
 * TarantoolClientFactory.configureClient(client)
 *          .withProxyMethodMapping(mapping -&gt; mapping.withDeleteFunctionName("custom_delete"))
 *          .build();
 *
 * </code>
 * </pre>
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientFactory {

    /**
     * Create a new client instance. Provides a builder interface.
     *
     * @return Tarantool client builder {@link TarantoolClientBuilder}
     */
    static TarantoolClientBuilder createClient() {
        return new TarantoolClientBuilderImpl();
    }

    /**
     * Configure an existing client instance and return a copy of it. Provides a builder interface.
     *
     * @param client client instance
     * @param <T>    configurator builder type
     * @return Tarantool client configurator {@link TarantoolClientConfigurator}
     */
    @SuppressWarnings("unchecked")
    static <T extends TarantoolClientConfigurator<T>> T configureClient(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return (T) new TarantoolClientConfiguratorImpl<>(client);
    }
}
