package io.tarantool.driver.api;

import io.netty.handler.ssl.SslContext;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategyType;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.MessagePackMapper;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder interface.
 * <p>
 * Provides a single entry point for building all types of Tarantool clients.
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientBuilder extends TarantoolClientConfigurator<TarantoolClientBuilder> {

    /**
     * Specify a single host of a Tarantool server. The default port 3301 will be used.
     *
     * @param host Tarantool server host
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host);

    /**
     * Specify a single host and a port of a Tarantool server.
     *
     * @param host Tarantool server host
     * @param port Tarantool server port
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host, int port);

    /**
     * Specify a Tarantool server address.
     *
     * @param socketAddress remote server address
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(InetSocketAddress socketAddress);

    /**
     * Specify one or more Tarantool server addresses.
     *
     * @param address list of addresses of Tarantool instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(TarantoolServerAddress... address);

    /**
     * Specify a list of Tarantool server addresses.
     * In a sharded cluster this is usually a list of router instances addresses.
     *
     * @param addressList list of Tarantool instance addresses
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(List<TarantoolServerAddress> addressList);

    /**
     * Specify a provider for Tarantool server addresses.
     * The typical usage of a provider is dynamic retrieving of the addresses from some configuration
     * or discovery manager like etcd, ZooKeeper, or a special Tarantool instance.
     * The actual list of addresses will then be retrieved each time a new connection is being established.
     *
     * @param addressProvider {@link TarantoolClusterAddressProvider}
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddressProvider(TarantoolClusterAddressProvider addressProvider);

    /**
     * Specify user credentials for authentication in a Tarantool server.
     * Important: these credentials will be used for all Tarantool server instances,
     * which addresses are returned by a service provider or are directly specified in the client configuration,
     * so make sure that all instances to be connected can authenticate with the specified credentials.
     *
     * @param tarantoolCredentials credentials for all Tarantool server instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withCredentials(TarantoolCredentials tarantoolCredentials);

    /**
     * Specify user credentials for password-based authentication in a Tarantool server.
     *
     * @param user     user to authenticate with
     * @param password password to authenticate with
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see #withCredentials(TarantoolCredentials tarantoolCredentials)
     */
    TarantoolClientBuilder withCredentials(String user, String password);

    /**
     * Specify the number of connections per one Tarantool server. The default value is 1.
     * More connections may help if a request can stuck on the server side or if the request payloads are big.
     * Important: the total number of open connections will be kept close to the specified number of connections times
     * the number of server addresses. In a normal working mode, the number of connections opened to each server will be
     * equal to this option value, however, an actual number may differ when the connections go down or up.
     *
     * @param connections the number of connections per one server
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnections(int connections);

    /**
     * Specify a configuration for mapping between Java objects and MessagePack entities.
     * <p>
     * This method takes a lambda as an argument, where the mapperBuilder is {@link DefaultMessagePackMapper.Builder}.
     * </p>
     * see {@link io.tarantool.driver.mappers.DefaultMessagePackMapperFactory}.
     *
     * @param mapperBuilder builder provider instance, e.g. a lambda function taking the builder
     *                      for {@link MessagePackMapper} instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setMessagePackMapper(MessagePackMapper)
     */
    TarantoolClientBuilder
    withDefaultMessagePackMapperConfiguration(UnaryOperator<MessagePackMapperBuilder> mapperBuilder);

    /**
     * Specify a mapper between Java objects and MessagePack entities.
     * The mapper contains converters for simple and complex tuple field types and for the entire tuples into custom
     * Java objects. This mapper is used by default if a custom mapper is not passed to a specific operation.
     * You may build and pass here your custom mapper or add some converters to a default one,
     * see {@link io.tarantool.driver.mappers.DefaultMessagePackMapperFactory}.
     *
     * @param mapper configured {@link MessagePackMapper} instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setMessagePackMapper(MessagePackMapper)
     */
    TarantoolClientBuilder withMessagePackMapper(MessagePackMapper mapper);

    /**
     * Specify a request timeout. The default is 2000 milliseconds.
     *
     * @param requestTimeout the timeout for receiving a response from the Tarantool server, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setRequestTimeout(int)
     */
    TarantoolClientBuilder withRequestTimeout(int requestTimeout);

    /**
     * Specify a connection timeout. The default is 1000 milliseconds.
     *
     * @param connectTimeout the timeout for connecting to the Tarantool server, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setConnectTimeout(int)
     */
    TarantoolClientBuilder withConnectTimeout(int connectTimeout);

    /**
     * Specify a response reading timeout. The default is 1000 milliseconds.
     *
     * @param readTimeout the timeout for reading the responses from Tarantool server, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setReadTimeout(int)
     */
    TarantoolClientBuilder withReadTimeout(int readTimeout);

    /**
     * Specify a custom connection selection strategy factory. A connection selection strategy encapsulates an algorithm
     * of selecting the next connection from the pool of available ones for performing the next request.
     *
     * @param connectionSelectionStrategyFactory connection selection strategy factory instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnectionSelectionStrategy(
            ConnectionSelectionStrategyFactory connectionSelectionStrategyFactory);

    /**
     * Select a built-in connection selection strategy factory. The default strategy types include simple round-robin
     * algorithms, good enough for balancing the requests between several connections with a single server (ROUND_ROBIN)
     * or multiple servers (PARALLEL_ROUND_ROBIN).
     *
     * @param connectionSelectionStrategyType built-in connection selection strategy factory type
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType connectionSelectionStrategyType);

    /**
     * Specify SslContext with settings for establishing SSL/TLS connection between Tarantool
     *
     * @param sslContext {@link SslContext} instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withSslContext(SslContext sslContext);

    /**
     * Specify a tarantool client config
     * <p>
     * It overrides previous settings for config
     * </p>
     *
     * @param config tarantool client config
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withTarantoolClientConfig(TarantoolClientConfig config);

    /**
     * Build the configured Tarantool client instance. Call this when you have specified all necessary settings.
     *
     * @return instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
