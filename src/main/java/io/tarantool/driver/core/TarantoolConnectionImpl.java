package io.tarantool.driver.core;

import io.netty.channel.Channel;
import io.tarantool.driver.TarantoolVersion;
import io.tarantool.driver.TarantoolVersionHolder;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class TarantoolConnectionImpl implements TarantoolConnection {

    private final TarantoolVersionHolder versionHolder;
    private final RequestFutureManager requestManager;
    private final Channel channel;
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final List<TarantoolConnectionFailureListener> failureListeners = new ArrayList<>();

    public TarantoolConnectionImpl(RequestFutureManager requestManager,
                                   TarantoolVersionHolder versionHolder,
                                   Channel channel) {
        this.requestManager = requestManager;
        this.versionHolder = versionHolder;
        this.channel = channel;
        channel.closeFuture().addListener(f -> {
           if (connected.compareAndSet(true, false)) {
               for (TarantoolConnectionFailureListener listener : failureListeners) {
                   listener.onConnectionFailure(f.cause());
               }
           }
        });
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
    public <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper) {
        if (!isConnected()) {
            throw new TarantoolClientException("Not connected to Tarantool server");
        }

        CompletableFuture<T> requestFuture = requestManager.submitRequest(request, resultMapper);
        channel.writeAndFlush(request).addListener(f -> {
            if (!f.isSuccess()) {
                requestFuture.completeExceptionally(
                        new RuntimeException("Failed to send the request to Tarantool server", f.cause()));
            }
        });

        return requestFuture;
    }

    @Override
    public void addConnectionFailureListener(TarantoolConnectionFailureListener listener) {
        failureListeners.add(listener);
    }

    @Override
    public void close() {
        connected.set(false);
        requestManager.close();
        channel.pipeline().close();
        channel.closeFuture().syncUninterruptibly();
    }
}
