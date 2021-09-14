package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public interface TarantoolClientFactory {

    static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> createDefaultClient() {
        return TarantoolClientFactory.createClient()
                .withDefaultCredentials()
                .withDefaultConnectionSelectionStrategy()
                .build();
    }

    static TarantoolClientBuilderFirstStep createClient() {
        return createClientTo(Collections.singletonList(new TarantoolServerAddress()));
    }

    static TarantoolClientBuilderFirstStep createClientTo(String address, int port) {
        return createClientTo(Collections.singletonList(new TarantoolServerAddress(address, port)));
    }

    static TarantoolClientBuilderFirstStep createClientTo(TarantoolServerAddress... address) {
        List<TarantoolServerAddress> addressList = Arrays.asList(address);
        if (addressList.isEmpty()) {
            return createClient();
        }
        return createClientTo(addressList);
    }

    static TarantoolClientBuilderFirstStep createClientTo(List<TarantoolServerAddress> addressList) {
        return new TarantoolClientBuilderFirstStepImpl(addressList);
    }
}
