package io.tarantool.driver;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.core.TarantoolChannelInitializer;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.TarantoolRequest;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class TarantoolConnectionImpl implements TarantoolConnection {

    private final TarantoolClientConfig config;
    private final Bootstrap bootstrap;
    private final InetSocketAddress serverAddress;
    private final TarantoolVersionHolder versionHolder;
    private final RequestFutureManager requestManager;
    private Channel channel;
    private AtomicBoolean connected;

    public TarantoolConnectionImpl(TarantoolClientConfig config,
                                   Bootstrap bootstrap,
                                   InetSocketAddress serverAddress) {

        this.config = config;
        this.bootstrap = bootstrap;
        this.serverAddress = serverAddress;
        this.versionHolder = new TarantoolVersionHolder();
        this.requestManager = new RequestFutureManager(config);
        this.connected = new AtomicBoolean(false);
    }

    @Override
    public CompletableFuture<TarantoolConnection> connect() throws TarantoolClientException {
        if (connected.compareAndSet(false, true)) {
            channel = null;
            CompletableFuture<Channel> connectionFuture = new CompletableFuture<>();
            ChannelFuture future = bootstrap
                    .handler(new TarantoolChannelInitializer(config, requestManager, versionHolder, connectionFuture))
                    .remoteAddress(serverAddress).connect();
            try {
                future.syncUninterruptibly();
            } catch (Throwable e) {
                connected.set(false);
                throw new TarantoolClientException(e);
            }
            if (!future.isSuccess()) {
                connected.set(false);
                throw new TarantoolClientException(
                        "Failed to connect to the Tarantool server in %d milliseconds", config.getConnectTimeout());
            }
            CompletableFuture<TarantoolConnection> conn = connectionFuture.thenApply(ch -> {
                this.channel = ch;
                return this;
            });
            return conn;
        } else {
            return CompletableFuture.completedFuture(this);
        }
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
        return connected.get() && channel != null;
    }

    @Override
    public <T> CompletableFuture<T> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper) {
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
    public void close() {
        if (connected.compareAndSet(true, false)) {
            channel.pipeline().close();
            channel.closeFuture().syncUninterruptibly();
        }
    }
}
