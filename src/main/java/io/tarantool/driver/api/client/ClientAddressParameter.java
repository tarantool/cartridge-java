package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.List;

public class ClientAddressParameter extends AbstractClientParameter<List<TarantoolServerAddress>> {

    public ClientAddressParameter(List<TarantoolServerAddress> value) {
        super(value);
    }

    @Override
    public ClientParameterType getParameterType() {
        return ClientParameterType.ADDRESS;
    }
}
