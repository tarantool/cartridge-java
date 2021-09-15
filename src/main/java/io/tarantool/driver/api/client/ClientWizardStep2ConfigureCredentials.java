package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolClusterAddressProvider;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;

import java.util.List;

public class ClientWizardStep2ConfigureCredentials {

    private final TarantoolClusterAddressProvider addressProvider;

    public ClientWizardStep2ConfigureCredentials(List<TarantoolServerAddress> addressList) {
        this.addressProvider = () -> addressList;
    }

    public ClientWizardStep3ConfigureConnectionStrategy withDefaultCredentials() {
        return withCredentials(new SimpleTarantoolCredentials());
    }

    public ClientWizardStep3ConfigureConnectionStrategy withCredentials(String user, String password) {
        return withCredentials(new SimpleTarantoolCredentials(user, password));
    }

    public ClientWizardStep3ConfigureConnectionStrategy withCredentials(SimpleTarantoolCredentials credentials) {
        return new ClientWizardStep3ConfigureConnectionStrategy(credentials, this.addressProvider);
    }
}
