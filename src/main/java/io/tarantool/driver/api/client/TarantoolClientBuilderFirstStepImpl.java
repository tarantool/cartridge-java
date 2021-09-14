package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.List;

public class TarantoolClientBuilderFirstStepImpl implements TarantoolClientBuilderFirstStep {

    private final TarantoolClusterAddressProvider addressProvider;

    public TarantoolClientBuilderFirstStepImpl(List<TarantoolServerAddress> addressList) {
        this.addressProvider = () -> addressList;
    }

    @Override
    public TarantoolClientBuilderSecondStep withDefaultCredentials() {
        return withCredentials(new SimpleTarantoolCredentials());
    }

    @Override
    public TarantoolClientBuilderSecondStep withCredentials(String user, String password) {
        return withCredentials(new SimpleTarantoolCredentials(user, password));
    }

    public TarantoolClientBuilderSecondStep withCredentials(SimpleTarantoolCredentials credentials) {
        return new TarantoolClientBuilderSecondStepImpl(credentials, this.addressProvider);
    }
}
