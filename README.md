# tarantool-java-driver
Java driver for Tarantool 1.10+ based on the [Netty framework](https://netty.io) and official [MessagePack serializer](https://github.com/msgpack/msgpack-java)

Usage example:
```java

class Scratch {
    public static void main(String[] args) {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
            .withCredentials(new SimpleTarantoolCredentials("admin", "1q2w3e"))
            .build();

        // using try-with-resources (auto close)
        try (StandaloneTarantoolClient client = new StandaloneTarantoolClient(config).connect("localhost", 3301)) {

            // built-in tuple type

            TarantoolResult<TarantoolTuple> tuples = client.space("test")
                // using index named "secondary"
                .select("secondary",
                        TarantoolIteratorType.ITER_ALL,
                        new TarantoolSelectOptions.Builder().withLimit(10).build())
                .get(); // using CompletableFuture in synchronous way

            // tuples can be iterated over
            tuples.forEach((t) -> System.out.println(String.format("Tuple ID=%d, name=%s",
                // Field interface with built-in primitive types conversions
                // Since each row can contain different number of fields, each field is wrapped in Optional
                t.getField(0).map(TarantoolField::getInteger).orElseThrow(RuntimeException::new),
                // Tuple interface for working with raw objects.
                // The mapper provided in config must contain a converter for the corresponding target type
                t.getObject(1, String.class).orElseThrow(RuntimeException::new))));

            // user-defined tuple type

            // using primary index
            TarantoolIndexQuery query = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY);
            TarantoolResult<CustomTuple> customTuples = client.space("test")
                .select(query,
                        // specifying select options is mandatory to avoid unwanted loading of too much data
                        // the default parameters are set to unlimited, though
                        new TarantoolSelectOptions(),
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
