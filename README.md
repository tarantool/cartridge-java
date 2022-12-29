<a href="http://tarantool.org">
   <img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250"
align="right">
</a>

# Java driver for Tarantool Cartridge

[![java-driver:ubuntu/master Actions Status](https://github.com/tarantool/cartridge-java/workflows/ubuntu-master/badge.svg)](https://github.com/tarantool/cartridge-java/actions)

Java driver for Tarantool Cartridge for Tarantool versions 1.10+ based on the asynchronous
[Netty](https://netty.io) framework and official
[MessagePack](https://github.com/msgpack/msgpack-java) serializer.
Provides CRUD APIs for seamlessly working with standalone Tarantool
servers and clusters managed by [Tarantool Cartridge](https://github.com/tarantool/cartridge)
with sharding via [vshard](https://github.com/tarantool/vshard).

## Quickstart

Example of single instance Tarantool application and java app connected using cartridge-java.

The easiest way to start experimenting with cartridge-java and single instance tarantool app is to use
[single instance test](/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java).
You can set breakpoints and run it in debug mode.
Testcontainers will start [single instance tarantool application](src/test/resources/single-instance.lua) for you.
So you will be able to manipulate data in Tarantool in real life through java expressions or Tarantool console.

If you want to start tarantool application manually all you need is to run this file in tarantool
``` bash
tarantool src/test/resources/single-instance.lua
```
Example of TarantoolClient set up
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L51-L59

Example of client API usage
https://github.com/tarantool/cartridge-java/blob/2f8e826deb9833a5deb6d21177527a46e8fdd039/src/test/java/io/tarantool/driver/integration/SingleInstanceExampleTest.java#L64-L79

You can read more about Cartridge applications in its [documentation](https://www.tarantool.io/ru/doc/latest/how-to/getting_started_cartridge/).
Also look at available Cartridge application [examples](https://github.com/tarantool/examples).

If you use this code in another project don't forget to add `cartridge-driver` dependency:
```xml
<dependency>
  <groupId>io.tarantool</groupId>
  <artifactId>cartridge-driver</artifactId>
  <version>0.10.0</version>
</dependency>
```
## Advanced usage
* [Cluster client](docs/ClusterTarantoolClient.md)  
  Connecting to Tarantool nodes.  
  The client can connect to storages and obtain data from spaces directly by space interface via native iproto binary protocol.  
  It may call lua storing procedures or eval lua code on nodes.
* [Proxy methods](docs/ProxyTarantoolClient.md)  
  Connecting to routers with crud library. It proxies space interface's methods to crud methods.
* [Retrying](docs/RetryingTarantoolClient.md)  
  Retrying transient cluster failures.
* [TarantoolTuple usage](docs/TarantoolTupleUsage.md)  
  Creating and operating with TarantoolTuple entity.
* [Custom sharding function](docs/CustomShardingFunction.md)  
  Determining bucket id on java side.
* [Thread control](docs/ThreadControl.md)  
  Specifying custom numbers of netty work threads.

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
