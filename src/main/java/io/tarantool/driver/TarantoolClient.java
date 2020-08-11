package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;
/**
 * Basic Tarantool client interface
 *
 * @author Alexey Kuzin
 */
public interface TarantoolClient extends AutoCloseable {
    /**
     * Connect the client to server
     *
     * @return configured connection to the Tarantool server
     * @throws TarantoolClientException if connection or client initialization fails
     */
    TarantoolConnection connect() throws TarantoolClientException;

    /**
     * Get the Tarantool client config passed to this client
     * @return {@link TarantoolClientConfig} instance
     */
    TarantoolClientConfig getConfig();
}
