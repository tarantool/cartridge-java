package io.tarantool.driver.api.client;

public interface ClientFactory {

    static ClientSettingsProvider createClient() {
        return new ClientSettingsProviderImpl();
    }

}
