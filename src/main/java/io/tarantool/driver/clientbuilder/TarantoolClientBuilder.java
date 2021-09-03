package io.tarantool.driver.clientbuilder;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;

public interface TarantoolClientBuilder<T, B extends TarantoolClientBuilder<T, B>> {

    T build();

    B withCredentials(String username, String password);

    B withCredentials(TarantoolCredentials credentials);

    B withAddress(TarantoolServerAddress address);

    B withAddress(String host, int port);

    B withAddressProvider(TarantoolClusterAddressProvider addressProvider);

    B withConfig(TarantoolClientConfig config);
}
