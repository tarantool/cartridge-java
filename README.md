# Java driver for Tarantool Cartridge

[![java-driver:ubuntu/master Actions Status](https://github.com/akudiyar/tarantool-cartridge-java-driver/workflows/ubuntu-master/badge.svg)](https://github.com/akudiyar/tarantool-cartridge-java-driver/actions)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the asynchronous
[Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.
Provides CRUD APIs for seamlessly working with standalone Tarantool
servers and clusters managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge)
with sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

Add the following dependency into your project:

```
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-driver</artifactId>
  <version>0.2.0</version>
</dependency>
```

### Standalone Tarantool client

Connects to a single Tarantool instance. Supports multiple connections.

See the following example of simple `StandaloneTarantoolClient` usage:

```java

class Scratch {
    public static void main(String[] args) {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
            .withCredentials(new SimpleTarantoolCredentials("admin", "1q2w3e"))
            .build();

        // using try-with-resources (auto close)
        try (StandaloneTarantoolClient client = new StandaloneTarantoolClient(config, "localhost", 3301)) {

            // built-in tuple type

            TarantoolResult<TarantoolTuple> tuples = client.space("test")
                // using index named "secondary"
                .select(Conditions.indexGreaterThan("secondary", Collections.singletonList(0)).withLimit(10))
                .get(); // using CompletableFuture in synchronous way

            // tuples can be iterated over
            tuples.forEach((t) -> System.out.println(String.format("Tuple ID=%d, name=%s",
                // Simple interface with built-in primitive types conversions
                t.getInteger(0),
                // Tuple interface for working with raw objects. Since the number of fields
                // can be variable, this method returns Optional.
                // The mapper provided in config must contain a converter for the corresponding target type
                // Getting field by name is supported if the space has defined schema
                t.getObject("name", String.class).orElseThrow(RuntimeException::new))));

            // user-defined tuple type

            // using primary index by default
            Conditions query = Conditions.any();
            TarantoolResult<CustomTuple> customTuples = client.space("test")
                .select(query,
                        // convert raw MessagePack array to object by hand
                        (v) -> new CustomTuple(v.get(0).asIntegerValue().asInt(), v.get(1).asStringValue().asString()))
                .get();

            customTuples.forEach(
                (t) -> System.out.println(String.format("Tuple ID=%d, name=%s", t.getId(), t.getName())));

        } catch (TarantoolClientException | IOException | InterruptedException | ExecutionException e) {
            // checked exceptions
            e.printStackTrace();
        }
    }

    private static class CustomTuple {
        private int id;
        private String name;

        CustomTuple(int id, String name) {
            this.id = id;
            this.name = name;
        }

        int getId() {
            return id;
        }

        String getName() {
            return name;
        }
    }
}

```

### Cluster Tarantool client

Connects to multiple Tarantool nodes, usually Tarantool Cartridge routers. Supports multiple connections.

The next example is showing the instantiation of the `ClusterTarantoolClient`
with binary discovery endpoint:

```java

class Scratch {
    public static void main(String[] args) {

        // Credentials for connecting to the discovery endpoint
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(USER_NAME, PASSWORD);

        BinaryClusterDiscoveryEndpoint endpoint = new BinaryClusterDiscoveryEndpoint.Builder()
                .withCredentials(credentials)
                // exposed API function from Tarantool endpoint
                .withEntryFunction("get_routers")
                .withServerAddress(new TarantoolServerAddress(
                        CartridgeHelper.getRouterHost(), CartridgeHelper.getRouterPort()))
                .build();

        TarantoolClusterDiscoveryConfig clusterDiscoveryConfig = new TarantoolClusterDiscoveryConfig.Builder()
                .withEndpoint(endpoint)
                .withReadTimeout(1000 * 5) // 5 seconds, timeout for reading the response body from the channel
                .withConnectTimeout(1000 * 5) // 5 seconds, timeout for connecting to the server
                // node information refresh delay
                .withDelay(1)
                .build();

        // Address provider is a customizable point for providing
        // server nodes addreses
        BinaryDiscoveryClusterAddressProvider addressProvider =
                new BinaryDiscoveryClusterAddressProvider(clusterDiscoveryConfig);

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5) // 5 seconds, timeout for receiving the server response
                .build();

        ClusterTarantoolClient client = new ClusterTarantoolClient(
                config, getBinaryProvider(), TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory.INSTANCE);

        ...

        client.close();
    }
}
```

### Proxy Tarantool client

A decorator for any of the basic client types. Allows connecting to instances with CRUD interfaces defined as user API
functions or as exposed [CRUD](https://github.com/tarantool/crud) functions. Works with tarantool/crud 0.2.0+.

See an example how to use the `ProxyTarantoolClient`:

```java

class Scratch {
    public static void main(String[] args) {

        // Cluster client is set up as described above
        ClusterTarantoolClient clusterClient = ...
        TarantoolClient client = new ProxyTarantoolClient(clusterClient);

        Conditions conditions = Conditions.greaterOrEquals("profile_id", 1_000_000);
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(20, selectResult.size());

        ...

        client.close();
    }
}
```

## Documentation

The Java Docs are available at [Github pages](https://akudiyar.github.io/tarantool-java-driver/).

If you have any questions about working with Tarantool, check out the
site [tarantool.io](https://tarantool.io/).

Feel free to ask questions about Tarantool and usage of this driver on
Stack Overflow with tag [tarantool](https://stackoverflow.com/questions/tagged/tarantool)
or join our community support chats in Telegram: [English](https://t.me/tarantool)
and [Russian](https://t.me/tarantool).

## [Changelog](https://github.com/akudiyar/tarantool-java-driver/blob/master/CHANGELOG.md)

## [License](https://github.com/akudiyar/tarantool-java-driver/blob/master/LICENSE)

## Requirements

Java 1.8 or higher is required for building and using this driver.

## Contributing

Contributions to this project are always welcome and highly encouraged.
