package io.tarantool.driver.clientfactory;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.clientbuilder.TarantoolClusterClientBuilder;

public class TarantoolClusterClientBuilderDecoratorImpl
        extends AbstractTarantoolClientBuilderDecorator<ClusterTarantoolTupleClient, TarantoolClusterClientBuilder>
        implements TarantoolClusterClientBuilderDecorator {

    public TarantoolClusterClientBuilderDecoratorImpl() {
        super.builder = ClusterTarantoolTupleClient.builder();
    }

    public TarantoolClusterClientBuilderDecoratorImpl(TarantoolClusterClientBuilder clusterBuilder) {
        super.builder = clusterBuilder;
    }

    @Override
    public ClusterTarantoolTupleClient build() {
        return builder.build();
    }

    public TarantoolClusterClientBuilderDecorator withCredentials(String username, String password) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withCredentials(username, password));
    }

    public TarantoolClusterClientBuilderDecorator withCredentials(TarantoolCredentials credentials) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withCredentials(credentials));
    }

    public TarantoolClusterClientBuilderDecorator withAddress(TarantoolServerAddress address) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withAddress(address));
    }

    public TarantoolClusterClientBuilderDecorator withAddress(String host, int port) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withAddress(host, port));
    }

    public TarantoolClusterClientBuilderDecorator withAddressProvider(TarantoolClusterAddressProvider addressProvider) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withAddressProvider(addressProvider));
    }

    public TarantoolClusterClientBuilderDecorator withConfig(TarantoolClientConfig config) {
        return new TarantoolClusterClientBuilderDecoratorImpl(super.builder.withConfig(config));
    }
}
