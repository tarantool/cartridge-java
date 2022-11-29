[Main page](../README.md)

# Cluster client

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
