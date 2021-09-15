package io.tarantool.driver.api.client;

public interface ClientWizardFactory {

    static ClientWizardStep1ConfigureAddressProvider wizard() {
        return new ClientWizardStep1ConfigureAddressProvider();
    }
}
