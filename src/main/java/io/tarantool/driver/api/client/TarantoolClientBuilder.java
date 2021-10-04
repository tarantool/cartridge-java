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
 * It can be used for creating client to tarantool with different parameters.
 *
 * @author Oleg Kuznetsov
 */
public interface TarantoolClientBuilder {

    /**
     * Specify addresses to tarantool instances
     *
     * @param host host of tarantool instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host);

    /**
     * Specify addresses to tarantool instances
     *
     * @param host host of tarantool instance
     * @param port port to tarantool instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(String host, int port);

    /**
     * Specify remote server address
     *
     * @param socketAddress remote server address
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddress(InetSocketAddress socketAddress);

    /**
     * Specify addresses to tarantool instances
     *
     * @param address list of addresses to tarantool instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(TarantoolServerAddress... address);

    /**
     * Specify addresses to tarantool instances
     *
     * @param addressList list of addresses to tarantool instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddresses(List<TarantoolServerAddress> addressList);

    /**
     * Specify address provider to tarantool instances
     *
     * @param addressProvider {@link TarantoolClusterAddressProvider}
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withAddressProvider(TarantoolClusterAddressProvider addressProvider);

    /**
     * Specify user credentials
     *
     * @param tarantoolCredentials credentials for instances
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withCredentials(TarantoolCredentials tarantoolCredentials);

    /**
     * Specify user credentials
     *
     * @param user     name for credentials
     * @param password password for credentials
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
     * Specify mapper between Java objects and MessagePack entities
     *
     * @param mapper configured {@link MessagePackMapper} instance
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setMessagePackMapper(MessagePackMapper)
     */
    TarantoolClientBuilder withMessagePackMapper(MessagePackMapper mapper);

    /**
     * Specify request timeout. Default is 2000 milliseconds
     *
     * @param requestTimeout the timeout for receiving a response from the Tarantool server, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setRequestTimeout(int)
     */
    TarantoolClientBuilder withRequestTimeout(int requestTimeout);

    /**
     * Specify connection timeout. Default is 1000 milliseconds
     *
     * @param connectTimeout the timeout for connecting to the Tarantool server, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     * @see TarantoolClientConfig#setConnectTimeout(int)
     */
    TarantoolClientBuilder withConnectTimeout(int connectTimeout);

    /**
     * Specify response reading timeout. Default is 1000 milliseconds
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
     * Specify lambda builder for proxying function names
     *
     * @param builder lambda with builder of proxy mapping
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
     * Specify number of attempts for requests retrying
     *
     * @param numberOfAttempts number of attempts for requests retrying
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryAttempts(int numberOfAttempts);

    /**
     * Specify delay in milliseconds between requests
     *
     * @param delay delay in milliseconds between requests
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryDelay(long delay);

    /**
     * Specify timeout between retrying requests
     *
     * @param requestTimeout timeout between retrying requests
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryTimeout(long requestTimeout);

    /**
     * Specify handler of exception for requests retrying
     *
     * @param callback handler of exception for requests retrying
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryExceptionCallback(Function<Throwable, Boolean> callback);

    /**
     * Specify timeout for the whole operation, in milliseconds
     *
     * @param operationTimeout timeout for the whole operation, in milliseconds
     * @return this instance of builder {@link TarantoolClientBuilder}
     */
    TarantoolClientBuilder withRequestRetryOperationTimeout(long operationTimeout);

    /**
     * Build the basic Tarantool client
     *
     * @return instance of tarantool tuple client {@link TarantoolClient}
     */
    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
