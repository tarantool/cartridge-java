package io.tarantool.driver.api;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolBadClientTypeException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.tarantool.driver.retry.TarantoolRequestRetryPolicies.retryNetworkErrors;

/**
 * Tarantool client builder implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderImpl implements TarantoolClientBuilder {

    private final TarantoolClientConfig.Builder configBuilder;

    private TarantoolClusterAddressProvider addressProvider;
    private RequestRetryPolicyFactory retryPolicyFactory;
    private ProxyOperationsMappingConfig mappingConfig;

    public TarantoolClientBuilderImpl() {
        this.configBuilder = TarantoolClientConfig.builder();
        this.addressProvider = () -> Collections.singleton(new TarantoolServerAddress());
    }

    public TarantoolClientBuilderImpl(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        this.configBuilder = new TarantoolClientConfig.Builder(client.getConfig());
        this.addressProvider = unpackClusterTarantoolTupleClient(client).getAddressProvider();

        try {
            this.mappingConfig = unpackProxyTarantoolTupleClient(client).getMappingConfig();
            this.retryPolicyFactory = unpackRetryingTarantoolTupleClient(client).getRetryPolicyFactory();
        } catch (TarantoolBadClientTypeException ignored) {
        }
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
            TarantoolConnectionSelectionStrategyType connectionSelectionStrategyType) {
        return withConnectionSelectionStrategy(connectionSelectionStrategyType.value());
    }

    @Override
    public TarantoolClientBuilder withConnectionSelectionStrategy(
            ConnectionSelectionStrategyFactory connectionSelectionStrategy) {
        this.configBuilder.withConnectionSelectionStrategyFactory(connectionSelectionStrategy);
        return this;
    }

    @Override
    public TarantoolClientBuilder withProxyMethodMapping() {
        return withProxyMethodMapping(UnaryOperator.identity());
    }

    @Override
    public TarantoolClientBuilder withProxyMethodMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        this.mappingConfig = builder.apply(ProxyOperationsMappingConfig.builder()).build();
        return this;
    }

    @Override
    public TarantoolClientBuilder withRetryingByNumberOfAttempts(int numberOfAttempts) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, UnaryOperator.identity());
    }

    @Override
    public TarantoolClientBuilder withRetryingByNumberOfAttempts(
            int numberOfAttempts, UnaryOperator<TarantoolRequestRetryPolicies
            .AttemptsBoundRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingByNumberOfAttempts(numberOfAttempts, retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> TarantoolClientBuilder withRetryingByNumberOfAttempts(
            int numberOfAttempts, T exceptionsCheck,
            UnaryOperator<TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                .builder(numberOfAttempts, exceptionsCheck)).build());
    }

    @Override
    public TarantoolClientBuilder withRetryingIndefinitely(UnaryOperator<TarantoolRequestRetryPolicies
            .InfiniteRetryPolicyFactory.Builder<Function<Throwable, Boolean>>> policy) {
        return withRetryingIndefinitely(retryNetworkErrors(), policy);
    }

    @Override
    public <T extends Function<Throwable, Boolean>> TarantoolClientBuilder withRetryingIndefinitely(
            T callback, UnaryOperator<TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.Builder<T>> policy) {
        return withRetrying(policy.apply(TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory.builder(callback))
                .build());
    }

    @Override
    public TarantoolClientBuilder withRetrying(RequestRetryPolicyFactory factory) {
        this.retryPolicyFactory = factory;
        return this;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client =
                new ClusterTarantoolTupleClient(this.configBuilder.build(), this.addressProvider);

        if (mappingConfig != null) {
            client = new ProxyTarantoolTupleClient(client, mappingConfig);
        }
        if (retryPolicyFactory != null) {
            client = new RetryingTarantoolTupleClient(client, this.retryPolicyFactory);
        }

        return client;
    }

    private ClusterTarantoolTupleClient unpackClusterTarantoolTupleClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        if (client instanceof ClusterTarantoolTupleClient) {
            return (ClusterTarantoolTupleClient) client;
        }
        if (client instanceof ProxyTarantoolTupleClient) {
            return unpackClusterTarantoolTupleClient(((ProxyTarantoolTupleClient) client).getClient());
        }
        if (client instanceof RetryingTarantoolTupleClient) {
            return unpackClusterTarantoolTupleClient(((RetryingTarantoolTupleClient) client).getClient());
        }
        throw new TarantoolBadClientTypeException(client.getClass());
    }

    private RetryingTarantoolTupleClient unpackRetryingTarantoolTupleClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        if (client instanceof RetryingTarantoolTupleClient) {
            return (RetryingTarantoolTupleClient) client;
        }
        if (client instanceof ProxyTarantoolTupleClient) {
            return unpackRetryingTarantoolTupleClient(((ProxyTarantoolTupleClient) client).getClient());
        }
        throw new TarantoolBadClientTypeException(client.getClass());
    }

    private ProxyTarantoolTupleClient unpackProxyTarantoolTupleClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        if (client instanceof ProxyTarantoolTupleClient) {
            return (ProxyTarantoolTupleClient) client;
        }
        if (client instanceof RetryingTarantoolTupleClient) {
            return unpackProxyTarantoolTupleClient(((RetryingTarantoolTupleClient) client).getClient());
        }
        throw new TarantoolBadClientTypeException(client.getClass());
    }
}
