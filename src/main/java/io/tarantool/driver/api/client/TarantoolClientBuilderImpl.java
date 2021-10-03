package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.client.ParameterType.ParameterGroup;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.retry.TarantoolRequestRetryPolicies.DEFAULT_ONE_HOUR_TIMEOUT;
import static io.tarantool.driver.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;

/**
 * Tarantool client builder implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderImpl implements TarantoolClientBuilder {

    private TarantoolClusterAddressProvider addressProvider;

    private final Map<ParameterType, Object> parameters;
    private final TarantoolClientConfig.Builder configBuilder;

    public TarantoolClientBuilderImpl() {
        this.parameters = new HashMap<>();
        this.configBuilder = TarantoolClientConfig.builder();
        this.addressProvider = () -> Collections.singleton(new TarantoolServerAddress());
    }

    @Override
    public TarantoolClientBuilder withAddress(String host) {
        return withAddresses(new TarantoolServerAddress(host));
    }

    @Override
    public TarantoolClientBuilder withAddress(String host, int port) {
        return withAddresses(new TarantoolServerAddress(host, port));
    }

    @Override
    public TarantoolClientBuilder withAddress(InetSocketAddress socketAddress) {
        return withAddresses(new TarantoolServerAddress(socketAddress));
    }

    @Override
    public TarantoolClientBuilder withAddresses(TarantoolServerAddress... address) {
        return withAddresses(Arrays.asList(address));
    }

    @Override
    public TarantoolClientBuilder withAddresses(List<TarantoolServerAddress> addressList) {
        return withAddressProvider(() -> addressList);
    }

    @Override
    public TarantoolClientBuilder withAddressProvider(TarantoolClusterAddressProvider addressProvider) {
        this.addressProvider = addressProvider;
        return this;
    }

    @Override
    public TarantoolClientBuilder withCredentials(String user, String password) {
        return withCredentials(new SimpleTarantoolCredentials(user, password));
    }

    @Override
    public TarantoolClientBuilder withCredentials(TarantoolCredentials credentials) {
        this.configBuilder.withCredentials(credentials);
        return this;
    }

    @Override
    public TarantoolClientBuilder withConnections(int numberOfConnections) {
        this.configBuilder.withConnections(numberOfConnections);
        return this;
    }

    @Override
    public TarantoolClientBuilder withMessagePackMapper(MessagePackMapper mapper) {
        this.configBuilder.withMessagePackMapper(mapper);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestTimeout(int requestTimeout) {
        this.configBuilder.withRequestTimeout(requestTimeout);
        return this;
    }

    @Override
    public TarantoolClientBuilder withConnectTimeout(int connectTimeout) {
        this.configBuilder.withConnectTimeout(connectTimeout);
        return this;
    }

    @Override
    public TarantoolClientBuilder withReadTimeout(int readTimeout) {
        this.configBuilder.withReadTimeout(readTimeout);
        return this;
    }

    @Override
    public TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType) {
        this.configBuilder.withConnectionSelectionStrategyFactory(tarantoolConnectionSelectionStrategyType.value());
        return this;
    }

    @Override
    public TarantoolClientBuilder withProxyMethodMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        parameters.put(ParameterType.PROXY_MAPPING, builder);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestRetryAttempts(int numberOfAttempts) {
        parameters.put(ParameterType.RETRY_ATTEMPTS, numberOfAttempts);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestRetryDelay(long delay) {
        parameters.put(ParameterType.RETRY_DELAY, delay);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestRetryTimeout(long requestTimeout) {
        parameters.put(ParameterType.REQUEST_TIMEOUT, requestTimeout);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestRetryExceptionCallback(Function<Throwable, Boolean> callback) {
        parameters.put(ParameterType.EXCEPTION_CALLBACK, callback);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestRetryOperationTimeout(long operationTimeout) {
        parameters.put(ParameterType.OPERATION_TIMEOUT, operationTimeout);
        return this;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = makeClusterClient();

        if (hasParameterInGroup(ParameterGroup.PROXY)) {
            client = makeProxyClient(client);
        }
        if (hasParameterInGroup(ParameterGroup.RETRY)) {
            client = makeRetryingClient(client);
        }

        return client;
    }

    /**
     * Check for user parameters belongs to group tarantool client parameters
     *
     * @param parameterGroup instance of {@link ParameterGroup}
     * @return true if at least one user parameter has in group, false if not
     */
    private boolean hasParameterInGroup(ParameterGroup parameterGroup) {
        for (ParameterType type : this.parameters.keySet()) {
            if (parameterGroup.hasParameterType(type)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create cluster client for tarantool with basic parameters
     *
     * @return new instance of {@link ClusterTarantoolTupleClient}
     */
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> makeClusterClient() {
        return new ClusterTarantoolTupleClient(this.configBuilder.build(), this.addressProvider);
    }

    /**
     * Create proxy client for tarantool with proxy parameters and decorated client
     *
     * @return new instance of {@link ProxyTarantoolTupleClient}
     */
    @SuppressWarnings("unchecked")
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> makeProxyClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        UnaryOperator<ProxyOperationsMappingConfig.Builder> value =
                (UnaryOperator<ProxyOperationsMappingConfig.Builder>) this.parameters.get(ParameterType.PROXY_MAPPING);
        ProxyOperationsMappingConfig config = value.apply(ProxyOperationsMappingConfig.builder()).build();

        return new ProxyTarantoolTupleClient(client, config);
    }

    /**
     * Create retrying client for tarantool with proxy parameters and decorated client
     *
     * @return new instance of {@link RetryingTarantoolTupleClient}
     */
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> makeRetryingClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return new RetryingTarantoolTupleClient(client, makeRetryPolicyFactory());
    }

    /**
     * Create retry policy factory by specified parameters
     *
     * @return {@link RequestRetryPolicyFactory}
     */
    private RequestRetryPolicyFactory makeRetryPolicyFactory() {
        Function<Throwable, Boolean> callback = getCallback();
        Integer numberOfAttempts = getNumberOfAttempts();
        Long requestTimeout = getRequestTimeout();
        Long delayMs = getDelay();

        if (numberOfAttempts > 0) {
            return TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                    .builder(numberOfAttempts, callback)
                    .withRequestTimeout(requestTimeout)
                    .withDelay(delayMs)
                    .build();
        }

        return TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory
                .builder(callback)
                .withDelay(delayMs)
                .withRequestTimeout(requestTimeout)
                .withOperationTimeout(getOperationTimeout())
                .build();
    }

    private Long getOperationTimeout() {
        return (Long) this.parameters.getOrDefault(ParameterType.OPERATION_TIMEOUT, DEFAULT_ONE_HOUR_TIMEOUT);
    }

    /**
     * Number of attempts value getter from parameters of builder
     *
     * @return number of attempts
     */
    private Integer getNumberOfAttempts() {
        return (Integer) this.parameters.getOrDefault(ParameterType.RETRY_ATTEMPTS, 0);
    }

    /**
     * Delay value in ms getter from parameters of builder
     *
     * @return delay in ms
     */
    private Long getDelay() {
        return (Long) this.parameters.getOrDefault(ParameterType.RETRY_DELAY, 0L);
    }

    /**
     * Request timeout getter from parameters of builder
     *
     * @return request timeout
     */
    private Long getRequestTimeout() {
        return (Long) this.parameters.getOrDefault(ParameterType.REQUEST_TIMEOUT, DEFAULT_ONE_HOUR_TIMEOUT);
    }

    /**
     * Exception handler getter from parameters of builder
     *
     * @return function for handling exceptions
     */
    @SuppressWarnings("unchecked")
    private Function<Throwable, Boolean> getCallback() {
        return (Function<Throwable, Boolean>)
                this.parameters.getOrDefault(ParameterType.EXCEPTION_CALLBACK, retryNetworkErrors());
    }
}
