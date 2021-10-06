package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

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
 * // Default Tarantool Client
 * TarantoolClientFactory.createClient().build();
 *
 * // Tarantool Cluster Tuple Client
 * TarantoolClientFactory.createClient()
 *             .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *             .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *             .build();
 *
 * // Tarantool Proxy Tuple Client
 * TarantoolClientFactory.createClient()
 *             .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *             .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *             .withProxyMethodMapping(builder -&gt; builder.withDeleteFunctionName("custom_delete"))
 *             .build();
 *
 * // Tarantool Retrying Tuple Client
 * TarantoolClientFactory.createClient()
 *            .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *            .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *            .withRetryingByNumberOfAttempts(5, throwable -&gt; throwable.getMessage().equals("Some error"),
 *                   policy -&gt; policy.withDelay(500)
 *                           .withRequestTimeout(1000)
 *            .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
 *            .build();
 *
 * // Tarantool Retrying Tuple Client with decorated Proxy client
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
 * // Configuring retry policy already created client
 * TarantoolClientFactory.configureClient(client)
 *           .withRetryingByNumberOfAttempts(5, throwable -&gt; throwable.getMessage().equals("Some error"),
 *                   policy -&gt; policy.withDelay(500)
 *                           .withRequestTimeout(1000)
 *           ).build();
 *
 * // Configuring proxy mapping already created client
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
     * Provides interface for Tarantool client building
     *
     * @return Tarantool client builder {@link TarantoolClientBuilder}
     */
    static TarantoolClientBuilder createClient() {
        return new TarantoolClientBuilderImpl();
    }

    /**
     * Provides interface for Tarantool client configuring
     *
     * @return Tarantool client configurator {@link TarantoolClientConfigurator}
     */
    static TarantoolClientConfigurator configureClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return new TarantoolClientConfiguratorImpl(client);
    }

}
