package io.tarantool.driver.integration;

import io.netty.channel.Channel;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionCloseListener;
import io.tarantool.driver.api.connection.TarantoolConnectionFailureListener;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

import org.msgpack.value.Value;

/**
 * @author Alexey Kuzin
 */
public class CustomConnection implements TarantoolConnection {

    private final TarantoolConnection connection;

    public CustomConnection(TarantoolConnection connection) {
        this.connection = connection;
    }

    @Override
    public void close() throws Exception {
        connection.getChannel().close();
    }

    @Override
    public InetSocketAddress getRemoteAddress() throws TarantoolClientException {
        return connection.getRemoteAddress();
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        return connection.getVersion();
    }

    @Override
    public boolean isConnected() {
        return connection.isConnected();
    }

    @Override
    public CompletableFuture<Value> sendRequest(TarantoolRequest request) {
        return connection.sendRequest(request);
    }

    @Override
    public Channel getChannel() {
        return connection.getChannel();
    }

    @Override
    public void addConnectionFailureListener(TarantoolConnectionFailureListener listener) {
        connection.addConnectionFailureListener(listener);
    }

    @Override
    public void addConnectionCloseListener(TarantoolConnectionCloseListener listener) {
        connection.addConnectionCloseListener(listener);
    }
}
