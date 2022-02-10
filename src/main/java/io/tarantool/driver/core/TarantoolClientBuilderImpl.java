package io.tarantool.driver.core;

import io.tarantool.driver.api.MessagePackMapperBuilder;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientBuilder;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.connection.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategyType;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.MessagePackMapper;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Tarantool client builder implementation.
 *
 * @author Oleg Kuznetsov
 */
public class TarantoolClientBuilderImpl extends TarantoolClientConfiguratorImpl<TarantoolClientBuilder>
        implements TarantoolClientBuilder {

    private final TarantoolClientConfig.Builder configBuilder;

    private TarantoolClientConfig config;
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
    public TarantoolClientBuilder
    withDefaultMessagePackMapperConfiguration(UnaryOperator<MessagePackMapperBuilder> mapperBuilder) {
        return withMessagePackMapper(mapperBuilder.apply(new DefaultMessagePackMapper.Builder()).build());
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
    public TarantoolClientBuilder withTarantoolClientConfig(TarantoolClientConfig config) {
        this.config = config;
        return this;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        TarantoolClientConfig config = this.config != null ? this.config : this.configBuilder.build();

        return super.decorate(new ClusterTarantoolTupleClient(config, this.addressProvider));
    }
}
