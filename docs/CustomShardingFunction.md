[Main page](../README.md)

# Custom sharding function

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

You can see the sources of this example in the [tests](../src/test/java/io/tarantool/driver/integration/proxy/options/ProxySpaceInsertOptionsIT.java).
