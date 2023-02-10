[Main page](../README.md)

# TarantoolTuple usage

You can use TarantoolTuple for creating tuple which can be sent to the Tarantool instance
or can be returned from default [tarantool/crud](https://github.com/tarantool/crud) or box methods.
You can create TarantoolTuple with this factory `TarantoolTupleFactory`.
See an example below:

https://github.com/tarantool/cartridge-java/blob/c9c985955f02bdcd0729c515ba00eb0ad8301089/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientExampleIT.java#L115-L162
