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

In this example I use custom delete function.
https://github.com/tarantool/cartridge-java/blob/3d30c6dcec6f88cabfdcdea01e9eed02614f3067/src/test/resources/cartridge/app/roles/api_router.lua#L165-L176
You can set up any client. In this case I use CRUD client.
https://github.com/tarantool/cartridge-java/blob/3d30c6dcec6f88cabfdcdea01e9eed02614f3067/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L143-L159
And reuse it then I need retrying client.
https://github.com/tarantool/cartridge-java/blob/3d30c6dcec6f88cabfdcdea01e9eed02614f3067/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L190-L215
You don't have to set up basic client if you need retying client only.
All methods of client builder with prefix `withRetrying` can be used with `createClient`.
  
In this code I call `delete_with_error_if_not_found` (custom delete function) before the record was inserted to the
database. So client recalls delete and removes the record after it was inserted.
https://github.com/tarantool/cartridge-java/blob/3d30c6dcec6f88cabfdcdea01e9eed02614f3067/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L85-L105
