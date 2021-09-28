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
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderImpl implements TarantoolClientBuilder {

    private final Map<ParameterType, Object> parameters;
    private final TarantoolClientConfig.Builder configBuilder;

    public TarantoolClientBuilderImpl() {
        this.parameters = new HashMap<>();
        this.configBuilder = TarantoolClientConfig.builder();
    }

    @Override
    public TarantoolClientBuilder withAddress(TarantoolServerAddress... address) {
        return withAddress(Arrays.asList(address));
    }

    @Override
    public TarantoolClientBuilder withAddress(List<TarantoolServerAddress> addressList) {
        return withAddressProvider(() -> addressList);
    }

    @Override
    public TarantoolClientBuilder withAddressProvider(TarantoolClusterAddressProvider addressProvider) {
        parameters.put(ParameterType.ADDRESS, addressProvider);
        return this;
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
        TarantoolClusterAddressProvider addressProvider = getAddressProvider();
        TarantoolClientConfig clientConfig = this.configBuilder.build();

        return new ClusterTarantoolTupleClient(clientConfig, addressProvider);
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
     * Address list getter from parameters of builder
     *
     * @return {@link List<TarantoolServerAddress>}
     */
    private TarantoolClusterAddressProvider getAddressProvider() {
        return (TarantoolClusterAddressProvider) this.parameters.getOrDefault(ParameterType.ADDRESS,
                (TarantoolClusterAddressProvider) () -> Collections.singletonList(new TarantoolServerAddress()));
    }

    /**
     * Create retry policy factory by specified parameters
     *
     * @return {@link RequestRetryPolicyFactory}
     */
    private RequestRetryPolicyFactory makeRetryPolicyFactory() {
        Long delayMs = getDelay();
        Integer numberOfAttempts = getNumberOfAttempts();
        Long operationTimeout = getOperationTimeout();
        Function<Throwable, Boolean> callback = getCallback();
        Long requestTimeout = getRequestTimeout();
        long infiniteRequestTimeout = requestTimeout < 0 ?
                TimeUnit.HOURS.toMillis(1) : requestTimeout;


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
                .withRequestTimeout(infiniteRequestTimeout)
                .withOperationTimeout(operationTimeout)
                .build();
    }

    private Long getOperationTimeout() {
        return (Long)
                this.parameters.getOrDefault(ParameterType.OPERATION_TIMEOUT, TimeUnit.HOURS.toMillis(1));
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
        return (Long) this.parameters.getOrDefault(ParameterType.REQUEST_TIMEOUT, 0L);
    }

    /**
     * Exception handler getter from parameters of builder
     *
     * @return function for handling exceptions
     */
    @SuppressWarnings("unchecked")
    private Function<Throwable, Boolean> getCallback() {
        return (Function<Throwable, Boolean>) this.parameters.getOrDefault(ParameterType.EXCEPTION_CALLBACK,
                (Function<Throwable, Boolean>) (t) -> true);
    }
}
