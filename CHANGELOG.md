# Changelog

## 1.0.0 (Unreleased)

### Features

 - Full support for Tarantool field types available in 1.10 and new
 field types introduced in 2.x: UUID, decimal, double
 - Customizable mapping between the protocol primitives (MsgPack) into POJOs
 - Default TarantoolTuple object with field access API
 - Seamless working with standalone and cluster Tarantool installations
 via StandaloneTarantoolCLient and ClusterTarantoolClient respectively.
 Cluster support is working via a proxy using via customizable API
 functions exposed on Tarantool router.
 - Built-in support for CRUD API (https://github.com/tarantool/crud)
 - Support for external server nodes discovery for cluster client with
 HTTP or binary discovery endpoint
