package io.tarantool.driver.clientfactory;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;

import java.util.concurrent.Executor;

public interface TarantoolClientBuilderDecorator<T> {

    T build();

    TarantoolRetryingClientBuilderDecorator withRetryPolicyFactory(RequestRetryPolicyFactory retryPolicyFactory);

    TarantoolProxyClientBuilderDecorator withMappingConfig(ProxyOperationsMappingConfig mappingConfig);

    TarantoolRetryingClientBuilderDecorator withExecutor(Executor executor);

    TarantoolClusterClientBuilderDecorator withCredentials(String username, String password);

    TarantoolClusterClientBuilderDecorator withCredentials(TarantoolCredentials credentials);

    TarantoolClusterClientBuilderDecorator withAddress(TarantoolServerAddress address);

    TarantoolClusterClientBuilderDecorator withAddress(String host, int port);

    TarantoolClusterClientBuilderDecorator withAddressProvider(TarantoolClusterAddressProvider addressProvider);

    TarantoolClusterClientBuilderDecorator withConfig(TarantoolClientConfig config);

}
