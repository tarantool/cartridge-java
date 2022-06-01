# Changelog

## [Unreleased]

- Add StringValueToByteArray converter ([#230](https://github.com/tarantool/cartridge-java/issues/230))

## [0.8.0] - 2022-05-20

### Features
- **[breaking change]** Use ValueType for registering ValueConverters ([#213](https://github.com/tarantool/cartridge-java/issues/213))
- Removed reflection in value converters search for performance ([#178](https://github.com/tarantool/cartridge-java/issues/178))
- Added objects converters for ArrayList and HashMap for increasing performance ([#207](https://github.com/tarantool/cartridge-java/issues/207))
- Fixed converting performance with targetType ([#212](https://github.com/tarantool/cartridge-java/issues/212))
- Added microbenchmark tests
- Added support for SSl/TLS ([#180](https://github.com/tarantool/cartridge-java/issues/180))
- Added support for IPv6 ([#199](https://github.com/tarantool/cartridge-java/issues/199))

### Bugfixes
- Fixed TarantoolClientConfig.Builder
- Fixed incorrect fieldNumber in the index metadata ([#203](https://github.com/tarantool/cartridge-java/issues/203))

## [0.7.2] - 2022-04-06

### Bugfixes
 - Fix CVE-2020-36518: jackson-databind bump to 2.12.6
 - Fix CVE-2021-43797: bump netty-all to 4.1.75.Final
 - Fix NPE in AbstractTarantoolConnectionManager when AddressProvider returns null

## [0.7.1] - 2022-04-06

### Features
 - Added new error: TarantoolAccessDeniedException

### Bugfixes
 - Fixed stackoverflow on comparing two null fields #150
 - Fixed overwriting endpoint by TarantoolClusterDiscoveryConfig.Builder.withEndpoint() #173
 - Fixed closing connections to nodes not returned by address provider #77
 - Fixed NPE in RetryingTarantoolClient #184


## [0.7.0] - 2022-02-21 

### Features
 - Added refresh() method in connection manager to reestablish lacking connections
 - Predefined discovery addressProviders now call refresh() when addresses change
 - Added new exception type TarantoolNoSuchProcedureException which is useful in retry policies (for example 
   to handle errors caused by not initialized instance states)
 - **Breaking change** error checking callbacks in retry policies now have Predicate type
 - Added method to client builder for easy messagepack mapper configuration
 - Added method to client builder which allows to use user defined TarantoolClientConfig

### Bugfixes
 - Concurrency issue in connection manager which caused connection creation over defined limit
 
## [Older]

### Features

 - Full support for Tarantool field types available in 1.10 and new field types introduced in 2.x: UUID, decimal, double
 - Customizable mapping between the protocol primitives (MsgPack) into POJOs
 - Default TarantoolTuple object with field access API
 - Seamless working with standalone and Cartridge cluster Tarantool instances.

Cluster support is available via a proxy using the customizable API functions exposed on Tarantool router.
 - Built-in support for CRUD API (https://github.com/tarantool/crud)
 - Schema fetching support for built-in spaces formats and DDL metadata (https://github.com/tarantool/ddl)
 - Built-in examples for external server nodes discovery with HTTP or binary discovery endpoints
 - Customizable request retry policies for reliable communication
 - Automatic reconnects on connection failure
