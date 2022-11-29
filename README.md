<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java driver for Tarantool Cartridge

[![java-driver:ubuntu/master Actions Status](https://github.com/tarantool/cartridge-java/workflows/ubuntu-master/badge.svg)](https://github.com/tarantool/cartridge-java/actions)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the asynchronous
[Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.
Provides CRUD APIs for seamlessly working with standalone Tarantool
servers and clusters managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge)
with sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

Example of single instance Tarantool application and java app connected using cartridge-java.

The easiest way to start experimenting with cartridge-java and single instance tarantool app is to use
[single instance test](/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java).
You can set breakpoints and run it in debug mode.
Testcontainers will start [single instance tarantool application](src/test/resources/single-instance.lua) for you.
So you will be able to manipulate data in Tarantool in real life through java expressions or Tarantool console.

If you want to start tarantool application manually all you need is to run this file in tarantool
``` bash
tarantool src/test/resources/single-instance.lua
```
Example of TarantoolClient set up
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L51-L59

Example of client API usage
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L64-L79

You can read more about Cartridge applications in its [documentation](https://www.tarantool.io/ru/doc/latest/how-to/getting_started_cartridge/).
Also look at available Cartridge application [examples](https://github.com/tarantool/examples).

If you use this code in another project don't forget to add `cartridge-driver` dependency:
```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-driver</artifactId>
  <version>0.9.2</version>
</dependency>
```

### Cluster Tarantool client

Connects to multiple Tarantool nodes, usually Tarantool Cartridge routers. Supports multiple connections to one node.
Cluster client connects to all specified nodes simultaneously and then routes all requests to different nodes using
the specified connection selection strategy. If any connection goes down, reconnection is performed automatically. In
this case, all dead connections are detected and re-established, but the alive ones remain untouched. When multiple
connections are open to a single host (using the `connections` option in the configuration), if one connection is
closed, all connections to that host are gracefully closed and re-established.

You may set up automatic retrieving of the list of cluster nodes available for connection (aka discovery). Discovery
provider variants with an HTTP endpoint and a stored function in Tarantool are available out-of-the-box. You may use
these variants or create your own discovery provider implementation. In real environments with high availability
requirements it is recommended to use an external configuration provider (like etcd), DNS or a balancing proxy for
connecting to the Tarantool server.

The next example shows the instantiation of the `TarantoolClient`
with stored function discovery provider:

```java
class Scratch {
    private static final String USER_NAME = "admin";
    private static final String PASSWORD = "secret-cluster-cookie";
    
    public static void main(String[] args) {
        // Credentials for connecting to the discovery endpoint
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);

        // Tarantool client config for connecting to the discovery endpoint
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(credentials)
                .build();

        // Discovery endpoint configuration
        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withClientConfig(config)
                // exposed API function from Tarantool endpoint
                .withEntryFunction("get_routers")
                // you may use any address provider for the discovery endpoint, even other discovery endpoint
                .withEndpointProvider(() -> Collections.singletonList(
                        new TarantoolServerAddress(container.getRouterHost(), container.getRouterPort())))
                .build();

        // Discovery algorithm configuration
        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withDelay(1) // node information refresh delay
                .build();

        // Address provider is a customizable point for providing server nodes addresses
        // BinaryDiscoveryClusterAddressProvider calls a stored function in a Tarantool instance, e.g. Cartridge router
        BinaryDiscoveryClusterAddressProvider addressProvider =
                new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig);

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withCredentials(USER_NAME, PASSWORD) // Credentials for connecting to the discovery endpoint
                .withAddressProvider(addressProvider)
                .withConnectTimeout(1000 * 5) // 5 seconds, timeout for connecting to the server
                .withReadTimeout(1000 * 5) // 5 seconds, timeout for reading the response body from the channel
                .withRequestTimeout(1000 * 5) // 5 seconds, timeout for receiving the server response
                // The chosen connection selection strategy will determine how hosts and connections are selected for 
                // performing the next request to the cluster
                .withConnectionSelectionStrategy(TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN)
                .build();

        // Mappers are used for converting MessagePack primitives to Java objects
        // The default mappers can be instantiated via the default mapper factory and further customized
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        // Use tuple factory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getConfig().getMessagePackMapper());
        // Space API provides CRUD operations for spaces
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace = client.space("test");
        TarantoolTuple tarantoolTuple;

        // Multiple requests are processed in an asynchronous way
        List<CompletableFuture<?>> allFutures = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            // If you are inserting tuples into a space sharded with tarantool/vshard, you will have to specify the
            // bucket_id field value or leave it as null
            tarantoolTuple = tupleFactory.create(1_000_000 + i, null, "FIO", 50 + i, 100 + i);
            allFutures.add(testSpace.insert(tarantoolTuple));
        }
        allFutures.forEach(CompletableFuture::join);

        client.close();
    }
}
```

### Proxy Tarantool client

This section shows the necessary settings for connecting to instances with a user-defined CRUD API exposed as Lua
stored functions or Cartridge roles implementing the API similar to the one of [tarantool/crud](https://github.com/tarantool/crud).
Works with `tarantool/crud` 0.3.0+.

See an example of how to set up a proxy client working with `tarantool/crud`:

```java
class Scratch {
    public static void main(String[] args) {

        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress("123.123.123.1")
                // use the value of cluster_cookie parameter in the init.lua file in your Cartridge application
                .withCredentials("admin", "secret-cluster-cookie")
                .withProxyMethodMapping()
                // also you may use a lambda function for specifying the proxy methods' names
                .withProxyMethodMapping(builder -> builder.withSelectFunctionName("customSelect"))
                .build();
        
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace = client.space("testSpace");

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getConfig().getMessagePackMapper());
        // Pass the field corresponding to bucket_id as null for tarantool/crud to compute it automatically
        TarantoolTuple tuple = tupleFactory.create(1_000_000, null, "profile_name");
        // Primary index key value will be determined from the tuple
        Conditions conditions = Conditions.after(tuple);
        TarantoolResult<TarantoolTuple> updateResult = testSpace.update(conditions, tuple).get();

        Conditions conditions = Conditions.greaterOrEquals("profile_id", 1_000_000);
        // crud.select(...) on the Cartridge router will be called internally
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(conditions).get();
        assertEquals(20, selectResult.size());

        // Any other operations with tuples as described in the examples above
        ...

        client.close();
    }
}
```

#### Using custom sharding function

A custom sharding function can be used to determine the bucket number - location in the cluster - and used further in the cluster operations.
For this purpose you need:
1) a hash function  
    As an example, a default function from tarantool/vshard - [crc32](https://www.tarantool.io/en/doc/latest/reference/reference_lua/digest/#lua-function.digest.crc32) with specific polynomial value.
    Java doesn't have crc32 out of the box with the ability to pass a polynomial value, so we'll implement our own:
    ```java
    private static long crc32(byte[] data) {
        BitSet bitSet = BitSet.valueOf(data);
        int crc32 = 0xFFFFFFFF; // initial value
        for (int i = 0; i < data.length * 8; i++) {
            if (((crc32 >>> 31) & 1) != (bitSet.get(i) ? 1 : 0)) {
                crc32 = (crc32 << 1) ^ 0x1EDC6F41; // xor with polynomial
            } else {
                crc32 = crc32 << 1;
            }
        }
        crc32 = Integer.reverse(crc32); // result reflect
        return crc32 & 0x00000000ffffffffL; // the unsigned java problem
    }
    ```
2) the number of buckets  
   This number can be obtained from Tarantool via `vshard.router.bucket_count` function out of [vshard module](https://github.com/tarantool/vshard)
    ```java
   public static <T extends Packable, R extends Collection<T>> Integer getBucketCount(
           TarantoolClient<T, R> client) throws ExecutionException, InterruptedException {
       if (!bucketCount.isPresent()) {
           bucketCount = Optional.ofNullable(
                   client.callForSingleResult("vshard.router.bucket_count", Integer.class).get()
           );
       }
       bucketCount.orElseThrow(() -> new TarantoolClientException("Failed to get bucket count"));
   }
    ```

Then we can determine bucket id by passing your key through hash function and get the remainder of the division by number of buckets:
```java
TarantoolTuple tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
byte[] key = getBytesFromList(Arrays.asList(tarantoolTuple.getInteger(0), tarantoolTuple.getInteger(2)));
Integer bucketId = (crc32(key) % getBucketCount(client)) + 1;
```

After that we may apply it in operations:
```java
InsertOptions insertOptions = ProxyInsertOptions.create().withBucketId(bucketId);
insertResult = profileSpace.insert(tarantoolTuple, insertOptions).get();

ProxySelectOptions selectOptions = ProxySelectOptions.create().withBucketId(bucketId);
selectResult = profileSpace.select(condition, selectOptions).get();
```

You can see the sources of this example in the [tests](src/test/java/io/tarantool/driver/integration/proxy/options/ProxySpaceInsertOptionsIT.java)

### Retrying Tarantool client

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

### TarantoolTuple usage
You can use TarantoolTuple for creating tuple which can be sent to the Tarantool instance
or can be returned from default "crud" functions.
You can create TarantoolTuple with this factory `TarantoolTupleFactory`.
See an example below:

```java
// Create a mapper factory  
DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
// Create a tuple factory
TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());

// Create a tuple from listed values: [1,2.0,'3',4]
TarantoolTuple tarantoolTuple = tupleFactory.create(1, 2.0, "3", new BigDecimal("4"));

Optional<?> object = tarantoolTuple.getObject(0);
Optional<Integer> integer = tarantoolTuple.getObject(0, Integer.class);

// Returned value will have 'double' type (it is used by default). 
Optional<?> doubleValue = tarantoolTuple.getObject(1);
// To get 'float' value we must explicitly define the target type.
Optional<?> floatValue = tarantoolTuple.getObject(1, Float.class);

Optional<?> stringValue = tarantoolTuple.getObject(2);

Optional<?> bigDecimalValue = tarantoolTuple.getObject(3);
```

## Thread control

For specific case maybe need specify custom numbers of netty work threads
First way is use client builder property 

```java

TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> setupClient() {
        return TarantoolClientFactory.createClient()
        .withCredentials("admin", "secret-cluster-cookie")
        .withAddress(container.getRouterHost(), container.getRouterPort())
        .withEventLoopThreadsNumber(threadsNumber)
        .build();
```        

Second way is use java system property

```
-Dio.netty.eventLoopThreads
```        

All this  properties should be used carefully, because if threads more than need may give low app performance  

## Documentation

The Java Docs are available at [Github pages](https://tarantool.github.io/cartridge-java/).

If you have any questions about working with Tarantool, check out the
site [tarantool.io](https://tarantool.io/).

Feel free to ask questions about Tarantool and usage of this driver on
Stack Overflow with tag [tarantool](https://stackoverflow.com/questions/tagged/tarantool)
or join our community support chats in Telegram: [English](https://t.me/tarantool)
and [Russian](https://t.me/tarantool).

## [Changelog](CHANGELOG.md)

## [License](LICENSE)

## Requirements

Java 1.8 or higher is required for building and using this driver.

## Building

1. Docker accessible to the current user is required for running integration tests.
2. Set up the right user for running Tarantool with in the container:
```bash
export TARANTOOL_SERVER_USER=<current user>
export TARANTOOL_SERVER_GROUP=<current group>
```
Substitute the user and group in these commands with the user and group under which the tests will run.
3. Use `./mvnw verify` to run unit tests and `./mvnw test -Pintegration` to run integration tests.
4. Use `./mvnw install` for installing the artifact locally.

## Contributing

Contributions to this project are always welcome and highly encouraged.
[See conventions for tests](docs/test-convention.md)
