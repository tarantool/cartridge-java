# Java driver for Tarantool Cartridge

[![java-driver:ubuntu/master Actions Status](https://github.com/tarantool/cartridge-java/workflows/ubuntu-master/badge.svg)](https://github.com/tarantool/cartridge-java/actions)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the asynchronous
[Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.
Provides CRUD APIs for seamlessly working with standalone Tarantool
servers and clusters managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge)
with sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

1. Set up your [Cartridge cluster](https://tarantool.io/cartridge). Use an existing Cartridge application or create
a new one from the available [examples](https://github.com/tarantool/examples). 
   
2. Add the [tarantool/crud](https://github.com/tarantool/crud) and [tarantool/ddl](https://github.com/tarantool/ddl)
modules to the dependencies in the [`rockspec`](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_dev/#creating-a-project)
file of your application.

3. Add the following lines into any [storage role](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_dev/#cluster-roles)
enabled on all storage instances in your cluster. The lines go into role API declaration section; in other words, into the returned table

```lua
return {
    role_name = 'app.roles.api_storage',
    init = init,
    ...
    get_schema = require('ddl').get_schema,
    ...
    dependencies = {
        'cartridge.roles.crud-storage'
    }
}
```

4. Add the following lines into any [router role](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_dev/#cluster-roles)
enabled on all router instances in your cluster to which the driver will be connected to:

```lua
...

-- Add the following variables
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

...

-- Add the following function
local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.api_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

...

local function init(opts)
    ...
    rawset(_G, 'ddl', { get_schema = get_schema }) -- Add this line
    ...
end
```

5. Check that at least one role enabled on the storage instances depends on the [`crud-storage`](https://github.com/tarantool/crud#api)
role from the `tarantool/crud` module and at least one role enabled on the router instances the driver will be connected
to depends on the [`crud-router`](https://github.com/tarantool/crud#api) role.

6. Start your Cartridge cluster. You may use [`cartridge start`](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_cli/)
for starting it manually or the [Testcontainers for Tarantool](https://github.com/tarantool/cartridge-testcontainers)
library for starting it automatically in tests.

7. Add the following dependency into your project:

```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-driver</artifactId>
  <version>0.4.3</version>
</dependency>
```

8. Create a new `TarantoolClient` instance:

```java
private ProxyTarantoolTupleClient setupClient() {
    TarantoolClientConfig config = TarantoolClientConfig.builder()
            // use the value of cluster_cookie parameter in the init.lua file in your Cartridge application
            .withCredentials(new SimpleTarantoolCredentials("admin", "secret-cluster-cookie"))
            .build();

    ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(config, ROUTER_HOST, ROUTER_PORT);
    return new ProxyTarantoolTupleClient(clusterClient);
}
```

9. Use the API provided by the Tarantool client, for example:

```java
    TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace = client.space("profile");

    List<Object> values = Arrays.asList(123, null, "Jane Doe", 18, 999);
    TarantoolTuple tarantoolTuple = tupleFactory.create(values);

    TarantoolResult<TarantoolTuple> insertTuples = profileSpace.insert(tarantoolTuple).get();
```

### Cluster Tarantool client

Connects to multiple Tarantool nodes, usually Tarantool Cartridge routers. Supports multiple connections to one node.
Cluster client connects to all specified nodes simultaneously and then routes all requests to different nodes using
the specified connection selection strategy. If any connection goes down, reconnection is performed automatically. In
this case, all dead connections are detected and re-established, but the alive ones remain untouched. When multiple
connections are open to a single host (using the `connections` option in the configuration), if one connection is
closed, all connections to that host are gracefully closed and re-established.

You may set up automatic retrieving of the list of cluster nodes available for connection (aka discovery). Discovery
provider variants with a HTTP endpoint and a stored function in Tarantool are available out-of-the-box. You may use
these variants or create your own discovery provider implementation. In real environments with high availability
requirements it is recommended to use an external configuration provider (like etcd), DNS or a balancing proxy for
connecting to the Tarantool server.

The next example is showing the instantiation of the `ClusterTarantoolTupleClient` with stored function discovery 
provider:

```java
class Scratch {
    public static void main(String[] args) {

        // Credentials for connecting to the discovery endpoint
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);

        // Tarantool client config for connecting to the discovery endpoint
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                .withCredentials(credentials)
                .build();

        // Discovery endpoint configuration
        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withConfig(config)
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

        // Actual Tarantool client config
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000 * 5) // 5 seconds, timeout for connecting to the server
                .withReadTimeout(1000 * 5) // 5 seconds, timeout for reading the response body from the channel
                .withRequestTimeout(1000 * 5) // 5 seconds, timeout for receiving the server response
                // The chosen connection selection strategy will determine how hosts and connections are selected for 
                // performing the next request to the cluster
                .withConnectionSelectionStrategyFactory(
                        TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory.INSTANCE)
                .build();

        ClusterTarantoolTupleClient client = new ClusterTarantoolTupleClient(config, addressProvider);

        // Mappers are used for converting MessagePack primitives to Java objects
        // The default mappers can be instantiated via the default mapper factory and further customized
        DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
        // Use tuple factory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getMessagePackMapper());
        // Space API provides CRUD operations for spaces
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace = client.space("test");
        TarantoolTuple tarantoolTuple;

        // Multiple requests are processed in an asynchronous way
        List<CompletableFuture<?>> allFutures = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            // If you are inserting tuples into a space sharded with tarantool/vshard, you will have to specify the
            // bucket_id field value or leave it as null
            tarantoolTuple = tupleFactory.create(1_000_000 + i, null, "FIO", 50 + i, 100 + i);
            allFutures.add(profileSpace.insert(tarantoolTuple));
        }
        allFutures.forEach(CompletableFuture::join);

        client.close();
    }
}
```

### Proxy Tarantool client

A decorator for any of the basic client types. Allows connecting to instances with CRUD interfaces defined as user-defined
stored functions or Cartridge roles implementing the API similar to the one of [tarantool/crud](https://github.com/tarantool/crud).
Works with `tarantool/crud` 0.3.0+.

See an example of how to use the `ProxyTarantoolClient`:

```java
class Scratch {
    public static void main(String[] args) {

        // Cluster client is set up as described above
        TarantoolClientConfig config = TarantoolClientConfig.builder()
                // use the value of cluster_cookie parameter in the init.lua file in your Cartridge application
                .withCredentials(new SimpleTarantoolCredentials("admin", "secret-cluster-cookie"))
                .build();

        ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(config, ROUTER_HOST, ROUTER_PORT);
        ProxyTarantoolTupleClient client = new ProxyTarantoolTupleClient(clusterClient);

        // Use TarantoolTupleFactory for instantiating new tuples
        TarantoolTupleFactory tupleFactory = new DefaultTarantoolTupleFactory(client.getMessagePackMapper());
        // Pass the field corresponding to bucket_id as null for tarantool/crud to compute it automatically
        TarantoolTuple tuple = tupleFactory.create(1_000_000, null, "profile_name");
        // Primary index key value will be determined from the tuple
        Conditions conditions = Conditions.after(tuple);
        TarantoolResult<TarantoolTuple> updateResult = profileSpace.update(conditions, tuple).get();

        Conditions conditions = Conditions.greaterOrEquals("profile_id", 1_000_000);
        // crud.select(...) on the Cartridge router will be called internally
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(20, selectResult.size());

        // Any other operations with tuples as described in the examples above
        ...

        client.close();
    }
}
```

### Retrying Tarantool client

For the cases of reliable communication with a Cartridge cluster under heavy load or in a case of some failure causing
unavailability of a part of the cluster nodes, the `RetryingTarantoolClient` class may be useful.

`RetryingTarantoolClient` allows to decorate any previously configured `TarantoolClient` instance, providing the request
retrying functionality with the specified retry policy. The request retry policy allows to specify the types of 
exceptions which may be retried. Some retry policies are available in the `TarantoolRequestRetryPolicies` class, 
but you may use your own implementations. See an example below:

```java
private ProxyTarantoolTupleClient setupClient() {
    TarantoolClientConfig config = TarantoolClientConfig.builder()
            .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
            .build();

    ClusterTarantoolTupleClient clusterClient = new ClusterTarantoolTupleClient(
            config, container.getRouterHost(), container.getRouterPort());
    return new ProxyTarantoolTupleClient(clusterClient);
}

private RetryingTarantoolTupleClient retrying(ProxyTarantoolTupleClient client, int retries) {
    return new RetryingTarantoolTupleClient(client,
            TarantoolRequestRetryPolicies.byNumberOfAttempts(
            retries, e -> e.getMessage().contains("Unsuccessful attempt")).build());
}

private RetryingTarantoolTupleClient retryingWithNetworkErrors(ProxyTarantoolTupleClient client, int retries) {
    return new RetryingTarantoolTupleClient(client,
            TarantoolRequestRetryPolicies.byNumberOfAttempts(
            retries, TarantoolRequestRetryPolicies.retryNetworkErrors()).build());
}

...

try (ProxyTarantoolTupleClient client = setupClient()) {
    String result = retrying(client, 4).callForSingleResult("retrying_function", String.class).get();
    assertEquals("Success", result);
    result = retryingWithNetworkErrors(client, 4).callForSingleResult("retrying_function", String.class).get();
    assertEquals("Success", result);
}
```

## Documentation

The Java Docs are available at [Github pages](https://tarantool.github.io/cartridge-java/).

If you have any questions about working with Tarantool, check out the
site [tarantool.io](https://tarantool.io/).

Feel free to ask questions about Tarantool and usage of this driver on
Stack Overflow with tag [tarantool](https://stackoverflow.com/questions/tagged/tarantool)
or join our community support chats in Telegram: [English](https://t.me/tarantool)
and [Russian](https://t.me/tarantool).

## [Changelog](https://github.com/tarantool/cartridge-java/blob/master/CHANGELOG.md)

## [License](https://github.com/tarantool/cartridge-java/blob/master/LICENSE)

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
