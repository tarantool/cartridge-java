package io.tarantool.driver.clientfactory;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.clientbuilder.TarantoolClientBuilder;
import io.tarantool.driver.clientbuilder.TarantoolClusterClientBuilder;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;

import java.util.concurrent.Executor;

public abstract class AbstractTarantoolClientBuilderDecorator<T, B extends TarantoolClientBuilder<T, B>>
        implements TarantoolClientBuilderDecorator<T> {

    protected TarantoolClusterClientBuilder builder;

    @Override
    public abstract T build();

    @Override
    public TarantoolClusterClientBuilderDecorator withCredentials(String username, String password) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withCredentials(username, password);
    }

    @Override
    public TarantoolClusterClientBuilderDecorator withCredentials(TarantoolCredentials credentials) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withCredentials(credentials);
    }

    @Override
    public TarantoolClusterClientBuilderDecorator withAddress(TarantoolServerAddress address) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withAddress(address);
    }

    @Override
    public TarantoolClusterClientBuilderDecorator withAddress(String host, int port) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withAddress(host, port);
    }

    @Override
    public TarantoolClusterClientBuilderDecorator withAddressProvider(TarantoolClusterAddressProvider addressProvider) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withAddressProvider(addressProvider);
    }

    @Override
    public TarantoolClusterClientBuilderDecorator withConfig(TarantoolClientConfig config) {
        return new TarantoolClusterClientBuilderDecoratorImpl().withConfig(config);
    }

    @Override
    public TarantoolProxyClientBuilderDecorator withMappingConfig(ProxyOperationsMappingConfig mappingConfig) {
        return new TarantoolProxyClientBuilderDecoratorImpl()
                .withMappingConfig(mappingConfig);
    }

    @Override
    public TarantoolRetryingClientBuilderDecorator withRetryPolicyFactory(RequestRetryPolicyFactory retryPolicyFactory) {
        return new TarantoolRetyingClientBuilderDecoratorImpl()
                .withRetryPolicyFactory(retryPolicyFactory);
    }

    @Override
    public TarantoolRetryingClientBuilderDecorator withExecutor(Executor executor) {
        return new TarantoolRetyingClientBuilderDecoratorImpl()
                .withExecutor(executor);
    }

    protected TarantoolClusterClientBuilder getBuilder() {
        if (builder == null) {
            return ClusterTarantoolTupleClient.builder();
        }
        return builder;
    }
}
