package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

public interface TarantoolConnection extends AutoCloseable {
    /**
     * Get the Tarantool server address for this connection
     * @return server address
     * @throws TarantoolClientException if the client is not connected
     */
    InetSocketAddress getRemoteAddress() throws TarantoolClientException;

    /**
     * Get the Tarantool server version
     * @return {@link TarantoolVersion}
     * @throws TarantoolClientException if the client is not connected
     */
    TarantoolVersion getVersion() throws TarantoolClientException;

    /**
     * Get the connection status
     * @return true, if the connection is alive
     */
    boolean isConnected();

    /**
     * Send a prepared request to the Tarantool server and flush the buffer
     * @param request the request
     * @param resultMapper the mapper for response body
     * @param <T> result type
     * @return result future
     */
    <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper);

    /**
     * Get the Netty channel baking this connection
     * @return channel
     */
    Channel getChannel();

    /**
     * Add a listener which is invoked when the connection is broken from the server side (e.g. server closed
     * the connection or a network failure has occurred).
     * @param listener a {@link TarantoolConnectionFailureListener} instance
     */
    void addConnectionFailureListener(TarantoolConnectionFailureListener listener);

    /**
     * Add a listener which is invoked when the connection is closed. The internal channel may probably be in an invalid
     * state at this moment.
     * @param listener a {@link TarantoolConnectionCloseListener} instance
     */
    void addConnectionCloseListener(TarantoolConnectionCloseListener listener);
}
