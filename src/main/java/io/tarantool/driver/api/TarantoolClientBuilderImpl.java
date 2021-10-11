package io.tarantool.driver.api;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.MessagePackMapper;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tarantool client builder implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderImpl extends TarantoolClientConfiguratorImpl<TarantoolClientBuilder>
        implements TarantoolClientBuilder {

    private final TarantoolClientConfig.Builder configBuilder;
    private TarantoolClusterAddressProvider addressProvider;

    public TarantoolClientBuilderImpl() {
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
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return super.decorate(new ClusterTarantoolTupleClient(this.configBuilder.build(), this.addressProvider));
    }
}
