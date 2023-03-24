[Main page](../README.md)

# SSL and mTLS usage examples
Iproto over SSL or mTLS supported only in [Tarantool Enterprise Edition](https://www.tarantool.io/en/product/enterprise/)
or [Tarantool Data Grid](https://www.tarantool.io/en/datagrid/).  
  
First of all, we need to have certificates and keys to use SSL.
For example we can generate them using [gen.sh](../src/test/resources/org/testcontainers/containers/enterprise/ssl/gen.sh)

## Set up SSL
To start Tarantool with SSL specify the `transport` type and path to ssl key and to the
ssl certificate. If you use a single Tarantool instance you can type them in `box.cfg`:
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/resources/org/testcontainers/containers/enterprise/ssl/ssl_server.lua#L1-L8

A connector client should be prepared for SSL connection.  
Generating `SslContext`: 
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/java/io/tarantool/driver/integration/ssl/SslClientITEnterprise.java#L123-L127

Setting `SslContext` using `withSslContext` method:
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/java/io/tarantool/driver/integration/ssl/SslClientITEnterprise.java#L104-L112

## Set up mTLS
To start Tarantool with mTLS we need to set `ssl_ca_file` beside the previous parameters:
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/resources/org/testcontainers/containers/enterprise/ssl/mtls/mtls_server.lua#L4-L7

Generating `SslContext` with mTLS:  
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/java/io/tarantool/driver/integration/ssl/SslClientMTlsITEnterprise.java#L108-L130

Setting `SslContext` using `withSslContext` method:
https://github.com/tarantool/cartridge-java/blob/a24657ec0c4f8610866f41cd0e6783c717f5d2bb/src/test/java/io/tarantool/driver/integration/ssl/SslClientMTlsITEnterprise.java#L75-L80

Also, data in `params` can be set through environment variables starting with `TARANTOOL_` for example
`params.transport` can be specified by setting `export TARANTOOL_TRANSPORT=ssl`.
