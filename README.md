# tarantool-java-driver
Java driver for Tarantool 1.10+ based on Netty framework

Example usage:
```java

class Scratch {
    public static void main(String[] args) {
        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(new SimpleTarantoolCredentials("admin", "1q2w3e"))
                .build();

        // using try-with-resources (auto close)
        try (StandaloneTarantoolClient client = new StandaloneTarantoolClient(config).connect("localhost", 3301)) {

            // Built-in tuple
            TarantoolResult<TarantoolTuple> tuples = client.space("test")
                    .select("secondary",
                            TarantoolIteratorType.ITER_ALL,
                            new TarantoolSelectOptions.Builder().withLimit(10).build())
                    .get();
            tuples.stream().forEach((t) -> System.out.println(String.format("Tuple ID=%d", (Integer) t.get(0).get().getValue())));

            // Custom tuple
            TarantoolIndexQuery query = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY);
            TarantoolResult<CustomTuple> customTuples = client.space("test")
                    .select(query,
                            new TarantoolSelectOptions.Builder().withLimit(10).build(),
                            (CustomTuple::new))
                    .get();
            customTuples.stream().forEach((t) -> System.out.println(String.format("Tuple ID=%d", t.getId())));
        } catch (TarantoolClientException | Exception e) {
            e.printStackTrace();
        }
    }

    public static class CustomTuple {
        private int id;

        public CustomTuple(ArrayValue v) {
            this.id = v.iterator().next().asIntegerValue().asInt();
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }
}

```
