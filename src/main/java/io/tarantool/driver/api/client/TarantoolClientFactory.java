package io.tarantool.driver.api.client;

/**
 * Tarantool client factory interface.
 * <p>
 * It provides a builder interface helping to create the basic Tarantool client types while hiding the internal client implementation details from the user. 
 *
 * <p>
 * Here are some examples of using this client factory:
 * <pre>
 * <code>
 *
 *     // Default Tarantool Client
 *     TarantoolClientFactory.createClient().build();
 *
 *     // Tarantool Cluster Tuple Client
 *     TarantoolClientFactory.createClient()
 *                 .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *                 .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *                 .build();
 *
 *     // Tarantool Proxy Tuple Client
 *     TarantoolClientFactory.createClient()
 *                 .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *                 .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *                 .withProxyMethodMapping(builder -&gt; builder.withDeleteFunctionName("createTest"))
 *                 .build();
 *
 *      // Tarantool Retrying Tuple Client
 *      TarantoolClientFactory.createClient()
 *                 .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *                 .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *                 .withRequestRetryDelay(500)
 *                 .withRequestRetryTimeout(4444)
 *                 .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
 *                 .build();
 *
 *       // Tarantool Retrying Tuple Client with decorated Proxy client
 *       TarantoolClientFactory.createClient()
 *                 .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
 *                 .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
 *                 .withRequestRetryAttempts(5)
 *                 .withRequestRetryDelay(500)
 *                 .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
 *                 .withProxyMethodMapping(builder -&gt; builder.withReplaceFunctionName("hello")
 *                         .withTruncateFunctionName("create"))
 *                 .withRequestRetryExceptionCallback(throwable -&gt; throwable.getMessage().equals("Hello World"))
 *                 .withRequestRetryTimeout(123)
 *                 .build();
 *
 * </code>
 * </pre>
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientFactory {

    /**
     * @return Tarantool client builder {@link TarantoolClientBuilder}
     */
    static TarantoolClientBuilder createClient() {
        return new TarantoolClientBuilderImpl();
    }

}
