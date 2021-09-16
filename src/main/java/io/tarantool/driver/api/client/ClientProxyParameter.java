package io.tarantool.driver.api.client;

import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.UnaryOperator;

public class ClientProxyParameter extends AbstractClientParameter<UnaryOperator<ProxyOperationsMappingConfig.Builder>> {

    public ClientProxyParameter(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        super(builder);
    }

    @Override
    public ClientParameterType getParameterType() {
        return ClientParameterType.PROXY;
    }
}
