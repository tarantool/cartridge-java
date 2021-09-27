package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.client.ParameterType.ParameterGroup;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder implementation
 */
public class TarantoolClientBuilderImpl implements TarantoolClientBuilder {

    private final TreeMap<ParameterType, Object> parameters;

    public TarantoolClientBuilderImpl() {
        this.parameters = new TreeMap<>();
    }

    @Override
    public TarantoolClientBuilder withAddress(TarantoolServerAddress... address) {
        parameters.put(ParameterType.ADDRESS, Arrays.asList(address));
        return this;
    }

    @Override
    public TarantoolClientBuilder withCredentials(TarantoolCredentials credentials) {
        parameters.put(ParameterType.CREDENTIALS, credentials);
        return this;
    }

    @Override
    public TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType) {
        parameters.put(ParameterType.CONNECTION_SELECTION_STRATEGY, tarantoolConnectionSelectionStrategyType);
        return this;
    }

    @Override
    public TarantoolClientBuilder withProxyMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        parameters.put(ParameterType.PROXY_MAPPING, builder);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRetryAttemptsInAmount(int numberOfAttempts) {
        parameters.put(ParameterType.RETRY_ATTEMPTS, numberOfAttempts);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRetryDelay(long delayMs) {
        parameters.put(ParameterType.RETRY_DELAY, delayMs);
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestTimeout(long requestTimeoutMs) {
        parameters.put(ParameterType.REQUEST_TIMEOUT, requestTimeoutMs);
        return this;
    }

    @Override
    public TarantoolClientBuilder withExceptionCallback(Function<Throwable, Boolean> callback) {
        parameters.put(ParameterType.EXCEPTION_CALLBACK, callback);
        return this;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = makeClusterClient();

        for (Map.Entry<ParameterType, Object> entry : parameters.entrySet()) {
            ParameterType type = entry.getKey();

            if (ParameterGroup.PROXY.hasParameterType(type)) {
                client = makeProxyClient(client);
            }

            if (ParameterGroup.RETRY.hasParameterType(type)) {
                client = makeRetryingClient(client);
            }
        }

        return client;
    }


    /**
     * Create cluster client for tarantool with basic parameters
     *
     * @return new instance of {@link ClusterTarantoolTupleClient}
     */
    private TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> makeClusterClient() {
        List<TarantoolServerAddress> addressList = getAddressList();
        TarantoolCredentials credentials = getCredentials();
        TarantoolClientConfig clientConfig = getConfig(credentials);

        return new ClusterTarantoolTupleClient(clientConfig, addressList);
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
     * Credentials getter from parameters of builder
     *
     * @return {@link TarantoolCredentials}
     */
    private TarantoolCredentials getCredentials() {
        return (TarantoolCredentials) this.parameters.getOrDefault(ParameterType.CREDENTIALS,
                new SimpleTarantoolCredentials());
    }

    /**
     * Address list getter from parameters of builder
     *
     * @return {@link List<TarantoolServerAddress>}
     */
    @SuppressWarnings("unchecked")
    private List<TarantoolServerAddress> getAddressList() {
        return (List<TarantoolServerAddress>) this.parameters.getOrDefault(ParameterType.ADDRESS,
                Collections.singletonList(new TarantoolServerAddress()));
    }

    /**
     * Client config getter from parameters of builder
     *
     * @return {@link TarantoolClientConfig}
     */
    private TarantoolClientConfig getConfig(TarantoolCredentials credentials) {
        TarantoolConnectionSelectionStrategyType selectionStrategy = (TarantoolConnectionSelectionStrategyType)
                this.parameters.getOrDefault(ParameterType.CONNECTION_SELECTION_STRATEGY,
                        TarantoolConnectionSelectionStrategyType.defaultType());

        return TarantoolClientConfig.builder()
                .withConnectionSelectionStrategyFactory(selectionStrategy.value())
                .withCredentials(credentials)
                .build();
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
                .build();
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
