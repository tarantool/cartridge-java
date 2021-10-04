package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder interface.
 * <p>
 * Provides a single entry point for building all types of Tarantool clients.
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientBuilder {

    /**
     * Specify a single host of a Tarantool server. The default port 3301 will be used.
     *
     * @param host Tarantool server host
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host);

    /**
     * Specify a single host and a port of a Tarantool server
     *
     * @param host Tarantool server host
     * @param port Tarantool server port
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host, int port);

    /**
     * Specify a Tarantool server address
     *
     * @param socketAddress remote server address
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(InetSocketAddress socketAddress);

    /**
     * Specify one or more Tarantool server addresses
     *
     * @param address list of addresses of Tarantool instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(TarantoolServerAddress... address);

    /**
     * Specify a list of Tarantool server addresses. In a sharded cluster this is usually a list of router instances addresses. 
     *
     * @param addressList list of Tarantool instance addresses
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(List<TarantoolServerAddress> addressList);

    /**
     * Specify a provider for Tarantool server addresses. The typical usage of a provider is dynamic retrieving of the addresses from some configuration or discovery manager like etcd, ZooKeeper, or a special Tarantool instance. The actual list of addresses will then be retrieved each time a new connection is being established.
     *
     * @param addressProvider {@link TarantoolClusterAddressProvider}
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddressProvider(TarantoolClusterAddressProvider addressProvider);

    /**
     * Specify user credentials for authentication in a Tarantool server. Important: these credentials will be used for all Tarantool server instances, which addresses are returned by a service provider or are directly specified in the client configuration, so make sure that all instances to be connected can authenticate with the specified credentials.
     *
     * @param tarantoolCredentials credentials for all Tarantool server instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withCredentials(TarantoolCredentials tarantoolCredentials);

    /**
     * Specify user credentials for password-based authentication in a Tarantool server.
     *
     * @see #withCredentials(TarantoolCredentials tarantoolCredentials)
     *
     * @param user     user to authenticate with
     * @param password password to authenticate with
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withCredentials(String user, String password);

    /**
     * Specify the number of connections used for sending requests to the server. The default value is 1.
     * More connections may help if a request can stuck on the server side or if the request payloads are big.
     *
     * @param connections the number of connections
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnections(int connections);

    /**
     * Specify a mapper between Java objects and MessagePack entities. The mapper contains converters for simple and complex tuple field types and for the entire tuples into custom Java objects. This mapper is used by default if a custom mapper is not passed to a specific operation. You may build and pass here your custom mapper or add some converters to a default one, see {@link DefaultMessagePackMapperFactory}.
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
     * Specify connection selection strategy
     *
     * @param tarantoolConnectionSelectionStrategyType type of connection selection strategy
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType);

    /**
     * Specify a builder provider for operations proxy configuration. This configuration allows specifying custom Lua function names callable on the Tarantool server, for replacing the default space operations with these functions calls. This allows, for example, replacing the default schema retrieving method or writing a custom "insert" implementation.
     *
     * @param builder builder provider instance, e.g. a lambda function taking the builder
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withProxyMethodMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    /**
     * Specify using proxy methods
     *
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withProxyMethodMapping();

    /**
     * Specify the number of retry attempts for each request
     *
     * @param numberOfAttempts number of retry attempts for each request
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryAttempts(int numberOfAttempts);

    /**
     * Specify a delay between consequent retries of the same request
     *
     * @param delay delay in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryDelay(long delay);

    /**
     * Specify a timeout for each request retry attempt
     *
     * @param requestTimeout request retry timeout, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryTimeout(long requestTimeout);

    /**
     * Specify an exception handler for determining the necessity of retrying the request. If the exception callback is not specified, the default behavior is retrying only the network exceptions (see TarantoolRequestRetryPolicies#retryNetworkErrors()).
     *
     * @param callback exception callback, returns {@code true} if the request must be retried
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryExceptionCallback(Function<Throwable, Boolean> callback);

    /**
     * Specify an overall timeout for all request retry attempts. Allows to not count the necessary number of retries, but limit the maximum time allowed for an operation.
     *
     * @param operationTimeout timeout for the whole operation, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryOperationTimeout(long operationTimeout);

    /**
     * Build the configured Tarantool client instance. Call this when you have specified all necessary settings.
     *
     * @return instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
