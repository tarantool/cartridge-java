package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;

import java.net.InetSocketAddress;

/**
 * Basic Tarantool client interface
 *
 * @author Alexey Kuzin
 */
public interface TarantoolClient extends AutoCloseable {
    /**
     * Connect the client to the specified address
     * @param address valid host name or IP address of a Tarantool server
     * @return configured connection to the Tarantool server
     * @throws TarantoolClientException if connection or client initialization fails
     */
    TarantoolConnection connect(InetSocketAddress address) throws TarantoolClientException;

    /**
     * Get the Tarantool client config passed to this client
     * @return {@link TarantoolClientConfig} instance
     */
    TarantoolClientConfig getConfig();
}
