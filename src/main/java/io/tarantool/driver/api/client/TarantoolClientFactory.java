package io.tarantool.driver.api.client;

/**
 * Tarantool client factory interface.
 * It helps to create basic clients for Tarantool instance.
 */
public interface TarantoolClientFactory {

    /**
     * @return Tarantool client builder {@link TarantoolClientBuilder}
     */
    static TarantoolClientBuilder createClient() {
        return new TarantoolClientBuilderImpl();
    }

}
