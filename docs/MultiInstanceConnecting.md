[Main page](../README.md)

# Connecting to multiple Tarantool instances

This section shows examples of how to connect to multiple [Cartridge](https://github.com/tarantool/cartridge) routers.  
We recommend to use load balancer in production applications rather than leaving this task to the connector.

> **Warning**<br>
In the case of using **multiple instances** of `TarantoolClient` we strongly recommend to apply a **shuffle** function to the list of addresses.
By default, `TarantoolClient` uses the [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) approach for selecting the next available connection to distribute the load between Tarantool servers.
When several instances of `TarantoolClient` are used simultaneously, with the default connection selection strategy and without **shuffling** of the server addresses the first request from every instance will be sent to the same server.

> **Note**<br>
You do not have to worry about shuffling addresses if you are using a single `TarantoolClient` connected to multiple instances, or if you are using a custom connection selection strategy that takes into account the problem of simultaneous requests.

## Setting addresses manually  
  
Example of `TarantoolClient` set up with connecting to multiple routers  
https://github.com/tarantool/cartridge-java/blob/d13e8693dcda041831f0a8de24092c98ebfff576/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L348-L359
https://github.com/tarantool/cartridge-java/blob/d13e8693dcda041831f0a8de24092c98ebfff576/src/test/java/io/tarantool/driver/integration/ReconnectIT.java#L373-L382

## Setting addresses with address provider
  
Example of `TarantoolClient` set up with address provider  
You may use the provided code as a reference for your own implementation  
https://github.com/tarantool/cartridge-java/blob/d13e8693dcda041831f0a8de24092c98ebfff576/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientWithAddressProviderExampleIT.java#L81-L89
Example of TarantoolClusterAddressProvider setup  
https://github.com/tarantool/cartridge-java/blob/d13e8693dcda041831f0a8de24092c98ebfff576/src/test/java/io/tarantool/driver/integration/ProxyTarantoolClientWithAddressProviderExampleIT.java#L44-L74
Do not forget to set tarantool function witch will return addresses  
The function must be global, so that it can be called from the connector
https://github.com/tarantool/cartridge-java/blob/09ec7084780c66e5457a8cd9e508376f40a266b7/src/test/resources/cartridge/app/roles/custom.lua#L3-L30
