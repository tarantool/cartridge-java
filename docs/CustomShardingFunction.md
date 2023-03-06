[Main page](../README.md)

# Custom sharding function

A custom sharding function can be used to determine the bucket number - location in the cluster - and used further in the cluster operations.
For this purpose you need:
1) a hash function  
   As an example, a default function from tarantool/vshard - [crc32](https://www.tarantool.io/en/doc/latest/reference/reference_lua/digest/#lua-function.digest.crc32) with specific polynomial value.
   Java doesn't have crc32 out of the box with the ability to pass a polynomial value, so we'll implement our own:
   https://github.com/tarantool/cartridge-java/blob/ef61296e2bec0f1fca3483225315590d614ea64d/src/test/java/io/tarantool/driver/integration/Utils.java#L93-L106
2) the number of buckets  
   This number can be obtained from Tarantool via `vshard.router.bucket_count` function out of [vshard module](https://github.com/tarantool/vshard)
   https://github.com/tarantool/cartridge-java/blob/ef61296e2bec0f1fca3483225315590d614ea64d/src/test/java/io/tarantool/driver/integration/Utils.java#L48-L56

Then we can determine bucket id by passing your key through hash function and get the remainder of the division by number of buckets:
https://github.com/tarantool/cartridge-java/blob/ef61296e2bec0f1fca3483225315590d614ea64d/src/test/java/io/tarantool/driver/integration/Utils.java#L68-L83

After that we may apply it in operations:
https://github.com/tarantool/cartridge-java/blob/ef61296e2bec0f1fca3483225315590d614ea64d/src/test/java/io/tarantool/driver/integration/proxy/options/ProxySpaceInsertOptionsIT.java#L142-L160
