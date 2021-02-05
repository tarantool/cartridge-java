package io.tarantool.driver.core;

import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Alexey Kuzin
 */
final class CustomConnection implements TarantoolConnection {

    private final String host;
    private final int port;
    private final AtomicInteger count = new AtomicInteger(0);
    private final AtomicBoolean connected = new AtomicBoolean(true);

    CustomConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void count() {
        count.incrementAndGet();
    }

    public int getCount() {
        return count.get();
    }

    @Override
    public InetSocketAddress getRemoteAddress() throws TarantoolClientException {
        return new InetSocketAddress(host, port);
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        return null;
    }

    public void setConnected(boolean connected) {
        this.connected.set(connected);
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper)
            throws TarantoolProtocolException {
        return null;
    }

    @Override
    public void addConnectionFailureListener(TarantoolConnectionFailureListener listener) {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public String toString() {
        return "CustomConnection{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", count=" + count +
                ", connected=" + connected +
                '}';
    }
}
