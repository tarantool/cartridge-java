# Changelog

## [Unreleased]

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
