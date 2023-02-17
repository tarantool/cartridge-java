# Changelog

## [Unreleased]
- Close public access to TarantoolResult*Impl ([#326](https://github.com/tarantool/cartridge-java/issues/326))
- Add deep copy instead of shallow copy in default message pack mapper
- Add factory builder to be able making mapper hierarchy

## [0.10.1] - 2023-01-13

### Internal and API changes
- Add ability to pass retrying executor in client builder ([#339](https://github.com/tarantool/cartridge-java/issues/339))
- Do not use sync calls instead of async in retrying ([#299](https://github.com/tarantool/cartridge-java/pull/299))

## [0.10.0] - 2022-12-29

### Features
- Add `fields` option to ProxySpace for controlling the result tuple fields ([#236](https://github.com/tarantool/cartridge-java/pull/236))
- Parse metadata from crud response ([#272](https://github.com/tarantool/cartridge-java/pull/272))

### Bugfixes
- Add parsing of the batch operation errors ([#334](https://github.com/tarantool/cartridge-java/issues/334))

### Internal and API changes
- Move metadata parsing to separate converters ([#325](https://github.com/tarantool/cartridge-java/pull/325))
- Add simple to use TarantoolTupleResultMapper factory ([#321](https://github.com/tarantool/cartridge-java/pull/321))
- Use netty part dependencies instead of netty-all ([#295](https://github.com/tarantool/cartridge-java/issues/295))
- Refactor mappers and split `TarantoolResultConverter` ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `TarantoolResultConverter` was removed, use `DefaultArrayValueToTarantoolResultConverter` or `DefaultMapValueToTarantoolTupleResultConverter` instead ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `ConverterWrapper` was moved to converters package ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `*MapperFactory` classes were moved to separate package ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `TarantoolTupleMultiResult` was renamed to `MultiValueTarantoolTupleResult` ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `TarantoolTupleSingleResult` was renamed to `SingleValueTarantoolTupleResult` ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `*MapperFactory` methods were renamed ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** `*DefaultConverter` converters moved into `mappers.converters.value.defaults` package ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- **[breaking change]** All converters from `mappers.converters.value.custom` moved into `mappers.converters.value` package ([#301](https://github.com/tarantool/cartridge-java/pull/301))
- Return back the `jmh-generator-annprocess` dependency

## [0.9.2] - 2022-11-15

### Features
- Adding default mapper for long arrays ([#290](https://github.com/tarantool/cartridge-java/pull/290))

### Internal and API changes
- Add dependency management ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump testcontainers-java-tarantool to 0.5.3 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump slf4j-api to 2.0.3 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump jackson-databind to 2.14.0 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump jackson-annotations to 2.14.0 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump org.testcontainers:junit-jupiter to 1.17.4 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Add junit class ordering according the order before junit-jupiter 5.8.0 ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Bump logback-classic to 1.3.4  ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Remove org.junit.jupiter:junit-jupiter  ([#296](https://github.com/tarantool/cartridge-java/pull/296))
- Remove jmh-generator-annprocess  ([#296](https://github.com/tarantool/cartridge-java/pull/296))

## [0.9.1] - 2022-10-13

### Features
- Enable Short to Integer converters ([#282](https://github.com/tarantool/cartridge-java/issues/282))

### Internal and API changes
- Changed TarantoolNullField class to singleton ([#195](https://github.com/tarantool/cartridge-java/pull/275))
- Bump netty to 4.1.78 ([#280](https://github.com/tarantool/cartridge-java/issues/280))

### Security
- Bump jackson-databind to 2.14.1-rc1 ([#284](https://github.com/tarantool/cartridge-java/pull/284))

## [0.9.0] - 2022-10-03

### Features
- Added options parameter to Tarantool Space API ([#266](https://github.com/tarantool/cartridge-java/pull/266))
- Added bucket id parameter to Tarantool Space API ([#270](https://github.com/tarantool/cartridge-java/pull/270))
- Added support for insert_many and replace_many CRUD operations ([#259](https://github.com/tarantool/cartridge-java/issues/259))

## [0.8.2] - 2022-09-16

### Features
- Added client EventLoopThreadsNumber property for control netty work threads ([#253](https://github.com/tarantool/cartridge-java/pull/253))

### Misc
- Removed code duplication in *ProxyOperations builders ([#256](https://github.com/tarantool/cartridge-java/issues/256))
- Refactor CRUDOperationOptions to a hierarchy of classes ([#258](https://github.com/tarantool/cartridge-java/issues/258))

## [0.8.1] - 2022-08-18

### Features
- Added StringValueToByteArray converter ([#230](https://github.com/tarantool/cartridge-java/issues/230))
- Added IPROTO constants to align code more with Tarantool

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
