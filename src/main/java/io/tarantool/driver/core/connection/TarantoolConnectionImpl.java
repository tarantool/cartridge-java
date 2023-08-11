package io.tarantool.driver.core.connection;

import io.netty.channel.Channel;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.api.connection.TarantoolConnection;
import io.tarantool.driver.api.connection.TarantoolConnectionCloseListener;
import io.tarantool.driver.api.connection.TarantoolConnectionFailureListener;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolRequestMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.TarantoolRequest;

import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class TarantoolConnectionImpl implements TarantoolConnection {

    protected final TarantoolVersionHolder versionHolder;
    protected final RequestFutureManager requestManager;
    protected final Channel channel;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final List<TarantoolConnectionFailureListener> failureListeners = new ArrayList<>();
    private final List<TarantoolConnectionCloseListener> closeListeners = new ArrayList<>();

    private static final Logger logger = LoggerFactory.getLogger(TarantoolConnection.class);

    public TarantoolConnectionImpl(
        RequestFutureManager requestManager,
        TarantoolVersionHolder versionHolder,
        Channel channel) {
        this.requestManager = requestManager;
        this.versionHolder = versionHolder;
        this.channel = channel;
        channel.closeFuture().addListener(f -> {
            if (connected.compareAndSet(true, false)) {
                for (TarantoolConnectionFailureListener listener : failureListeners) {
                    listener.onConnectionFailure(this, f.cause());
                }
            }
        });
    }

    @Override
    public InetSocketAddress getRemoteAddress() throws TarantoolClientException {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        if (!isConnected()) {
            throw new TarantoolClientException("Not connected to Tarantool server");
        }
        return versionHolder.getVersion();
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public TarantoolRequestMetadata sendRequest(TarantoolRequest request) {
        if (!isConnected()) {
            throw new TarantoolClientException("Not connected to Tarantool server");
        }

        TarantoolRequestMetadata requestMetadata = requestManager.submitRequest(request);
        CompletableFuture<Value> requestFuture = requestMetadata.getFuture();
        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                requestFuture.completeExceptionally(
                    new RuntimeException("Failed to send the request to Tarantool server", f.cause()));
            } else {
                logger.debug("Request {} sent, status Success", request.getHeader().getSync());
            }
        });

        return requestMetadata;
    }

    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public void addConnectionFailureListener(TarantoolConnectionFailureListener listener) {
        failureListeners.add(listener);
    }

    @Override
    public void addConnectionCloseListener(TarantoolConnectionCloseListener listener) {
        closeListeners.add(listener);
    }

    @Override
    public void close() {
        connected.set(false);
        for (TarantoolConnectionCloseListener listener : closeListeners) {
            listener.onConnectionClosed(this);
        }
        requestManager.close();
        channel.pipeline().close();
        channel.closeFuture().syncUninterruptibly();
    }
}
