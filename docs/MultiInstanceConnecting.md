[Main page](../README.md)

# Connecting to multiple Tarantool instances

This section shows examples of how to connect to multiple [Cartridge](https://github.com/tarantool/cartridge) routers.  
We recommend to use balancer in production applications rather than leaving this task to the connector.

## Setting addresses manually  
  
Example of TarantoolClient set up with connecting to multiple routers  
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L291-L301
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L315-L321

## Setting addresses with address provider
  
Example of TarantoolClient set up with address provider  
You may use the provided code as a reference for your own implementation  
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientWithAddressProviderExampleIT.java#L75-L82
Example of TarantoolClusterAddressProvider setup  
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientWithAddressProviderExampleIT.java#L43-L68
Do not forget to set tarantool function witch will return addresses  
The function must be global, so that it can be called from the connector
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/resources/cartridge/app/roles/custom.lua#L3-L30
