[Main page](../README.md)

# Proxy Tarantool client

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
