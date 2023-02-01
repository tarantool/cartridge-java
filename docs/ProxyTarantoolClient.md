[Main page](../README.md)

# Proxy Tarantool client

This section shows the necessary settings for connecting to instances with a user-defined CRUD API exposed as Lua
stored functions or Cartridge roles implementing the API similar to the one of [tarantool/crud](https://github.com/tarantool/crud).
Works with `tarantool/crud` 0.3.0+.

Example of TarantoolClient set up  
https://github.com/tarantool/cartridge-java/blob/8a880423da1ce2bc0e82557d70ab46c9e7eba618/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientExampleIT.java#L45-L55

Example of client API usage  
https://github.com/tarantool/cartridge-java/blob/8a880423da1ce2bc0e82557d70ab46c9e7eba618/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientExampleIT.java#L64-L107

You can read more about Cartridge applications in its [documentation](https://www.tarantool.io/ru/doc/latest/how-to/getting_started_cartridge/).  
Also look at available Cartridge application [examples](https://github.com/tarantool/examples).