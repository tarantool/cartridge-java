[Main page](../README.md)

# Retrying client

For the cases of reliable communication with a Cartridge cluster under heavy load or in a case of some failure causing
unavailability of a part of the cluster nodes, the methods of client builder with prefix `withRetrying` may be useful.

The request retry policy allows specifying the types of exceptions that may be retried.
By default, failed requests will be repeated only for some known network problems, such as
`TimeoutException`, `TarantoolConnectionException` and `TarantoolInternalNetworkException`.
Some retry policies are available in the `TarantoolRequestRetryPolicies` class, but you may use your own implementations.
If you want to use proxy calls or retry settings only for a number of requests, you may use configureClient(client)
in `TarantoolClientFactory` for making a new configured client instance. Note, that the new instance will share the same
connection pool and basic client settings, and only augment the behavior of the client.
See an example below:

```java

TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
    return TarantoolClientFactory.createClient()
        .withCredentials("admin", "secret-cluster-cookie")
        .withAddress(container.getRouterHost(), container.getRouterPort())
        .withProxyMethodMapping()
        .build();
}

TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> retrying(
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client, int retries, long delay) {
        return TarantoolClientFactory.configureClient(client)
                    .withRetryingByNumberOfAttempts(
                    retries,
                    // you can use default predicates from TarantoolRequestRetryPolicies for checking errors
                    TarantoolRequestRetryPolicies.retryNetworkErrors()
                    // also you can use your own predicates and combine them with each other or with defaults
                        .or(e -> e.getMessage().contains("Unsuccessful attempt"))
                        .or(TarantoolRequestRetryPolicies.retryTarantoolNoSuchProcedureErrors()),
                    policy -> policy.withDelay(delay))
                    .build();
        }

...

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = setupClient();
    String result = retrying(client, 4, 500).callForSingleResult("retrying_function", String.class).get();
    assertEquals("Success", result);
    result = retrying(client, 2, 1000).callForSingleResult("retrying_function", String.class).get();
    assertEquals("Success", result);
```
