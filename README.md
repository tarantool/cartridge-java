<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java driver for Tarantool Cartridge

[![Tests Community](https://github.com/tarantool/cartridge-java/actions/workflows/tests-ce.yml/badge.svg)](https://github.com/tarantool/cartridge-java/actions/workflows/tests-ce.yml)
[![Tests Enterprise](https://github.com/tarantool/cartridge-java/actions/workflows/tests-ee.yml/badge.svg)](https://github.com/tarantool/cartridge-java/actions/workflows/tests-ee.yml)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the asynchronous
[Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.
Provides CRUD APIs for seamlessly working with standalone Tarantool
servers and clusters managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge)
with sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

Here is an easy way to try using Tarantool together with this Java driver.

Look at the [single instance test](/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleIT.java) to start
experimenting with a single Tarantool server instance and a simple Java app.
You can set breakpoints and run it in "debug" mode using your IDE.
Testcontainers library will start a [single instance Tarantool server](src/test/resources/single-instance.lua) for you.
Try to manipulate data in Tarantool in real time through evaluating Java expressions in debug mode or using the Tarantool
console.

If you want to start a simple Tarantool application manually, all you need is to install Tarantool and run it with this file:
``` bash
tarantool src/test/resources/single-instance.lua
```

Example of creating a `TarantoolClient` instance:
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L51-L59

Example of the `TarantoolClient` API usage:
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L64-L79

If you use this code in another project don't forget to add a `cartridge-driver` dependency:
```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-driver</artifactId>
  <version>0.13.0</version>
</dependency>
```
## Advanced usage
* [Using CRUD stored procedures](docs/ProxyTarantoolClient.md)  
  Connecting to routers with [tarantool/crud](https://github.com/tarantool/crud) library.
  It proxies space interface's methods to CRUD methods.
* [Connecting to multiple instances](docs/MultiInstanceConnecting.md)  
  Connecting to multiple Tarantool instances.
* [Retrying](docs/RetryingTarantoolClient.md)  
  Retrying transient cluster failures.
* [TarantoolTuple usage](docs/TarantoolTupleUsage.md)  
  Creating and operating with TarantoolTuple entity.
* [Mappers and converters](docs/MappersAndConverters.md)  
  Converting data from MessagePack types to basic Java types and back.
* [Token authorization](docs/TokenAuthorization.md)  
  An example of TDG authorization by token
* [Custom sharding function](docs/CustomShardingFunction.md)  
  Determining bucket id on java side to send requests directly on storage.
* [Thread control](docs/ThreadControl.md)  
  Specifying custom numbers of netty work threads.
* [Space not found](docs/SpaceNotFound.md)  
  The problem of getting metadata.
* [SSL and mTLS usage examples (enterprise edition only)](docs/SslAndMtls.md)  
  Connect to Tarantool instances over an SSL or mTLS channel.

## Documentation

The Java Docs are available at [Github pages](https://tarantool.github.io/cartridge-java/).

If you have any questions about working with Tarantool, check out the
site [tarantool.io](https://tarantool.io/).

Feel free to ask questions about Tarantool and usage of this driver on
Stack Overflow with tag [tarantool](https://stackoverflow.com/questions/tagged/tarantool)
or join our community support chats in Telegram: [English](https://t.me/tarantool)
and [Russian](https://t.me/tarantool).

## [Changelog](CHANGELOG.md)

## [License](LICENSE)

## Requirements

Java 1.8 or higher is required for building and using this driver.

## Building

1. Docker accessible to the current user is required for running integration tests.
2. Set up the right user for running Tarantool with in the container:
```bash
export TARANTOOL_SERVER_USER=<current user>
export TARANTOOL_SERVER_GROUP=<current group>
```
Substitute the user and group in these commands with the user and group under which the tests will run.
3. Use `./mvnw verify` to run unit tests and `./mvnw test -Pintegration` to run integration tests.
4. Use `./mvnw install` for installing the artifact locally.

## Contributing

Contributions to this project are always welcome and highly encouraged.
[See conventions for tests](docs/test-convention.md)
