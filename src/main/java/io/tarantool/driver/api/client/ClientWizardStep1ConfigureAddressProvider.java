package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ClientWizardStep1ConfigureAddressProvider {

    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> createDefaultClient() {
        return withDefaultAddress()
                .withDefaultCredentials()
                .withDefaultConnectionSelectionStrategy()
                .build();
    }

    ClientWizardStep2ConfigureCredentials withDefaultAddress() {
        return withAddresses(Collections.singletonList(new TarantoolServerAddress()));
    }

    ClientWizardStep2ConfigureCredentials withAddress(String address, int port) {
        return withAddresses(Collections.singletonList(new TarantoolServerAddress(address, port)));
    }

    ClientWizardStep2ConfigureCredentials withAddresses(TarantoolServerAddress... address) {
        List<TarantoolServerAddress> addressList = Arrays.asList(address);
        if (addressList.isEmpty()) {
            return withDefaultAddress();
        }
        return withAddresses(addressList);
    }

    ClientWizardStep2ConfigureCredentials withAddresses(List<TarantoolServerAddress> addressList) {
        return new ClientWizardStep2ConfigureCredentials(addressList);
    }

}
