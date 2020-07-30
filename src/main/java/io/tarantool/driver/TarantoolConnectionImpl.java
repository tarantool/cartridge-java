package io.tarantool.driver;

import io.netty.channel.Channel;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class TarantoolConnectionImpl implements TarantoolConnection {

    private final TarantoolClientConfig config;
    private final TarantoolVersionHolder versionHolder;
    private final RequestFutureManager requestManager;
    private final Channel channel;
    private final TarantoolMetadataOperations metadata;
    private AtomicBoolean connected;

    public TarantoolConnectionImpl(TarantoolClientConfig config,
                                   TarantoolVersionHolder versionHolder,
                                   RequestFutureManager requestManager,
                                   Channel channel) {

        this.config = config;
        this.versionHolder = versionHolder;
        this.requestManager = requestManager;
        this.channel = channel;
        this.connected = new AtomicBoolean(true);
        this.metadata = new TarantoolMetadata(config, this);
    }

    @Override
    public TarantoolVersion getVersion() throws TarantoolClientException {
        if (isClosed()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        return versionHolder.getVersion();
    }

    @Override
    public TarantoolSpaceOperations space(String spaceName) throws TarantoolClientException {
        Assert.hasText(spaceName, "Space name must not be null or empty");

        if (isClosed()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        Optional<TarantoolSpaceMetadata> meta = this.metadata().getSpaceByName(spaceName);
        if (!meta.isPresent()) {
            throw new TarantoolSpaceNotFoundException(spaceName);
        }
        return new TarantoolSpace(meta.get().getSpaceId(), config, this, requestManager);
    }

    @Override
    public TarantoolSpaceOperations space(int spaceId) throws TarantoolClientException {
        Assert.state(spaceId > 0, "Space ID must be greater than 0");

        if (isClosed()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        return new TarantoolSpace(spaceId, config, this, requestManager);
    }

    @Override
    public TarantoolMetadataOperations metadata() throws TarantoolClientException {
        if (isClosed()) {
            throw new TarantoolClientException("The client is not connected to Tarantool server");
        }
        return metadata;
    }

    @Override
    public <T> CompletableFuture<T> call(String functionName, Object... arguments) throws TarantoolClientException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> CompletableFuture<T> call(String functionName, List<Object> arguments,
                                         MessagePackValueMapper resultMapper) throws TarantoolClientException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> CompletableFuture<T> call(String functionName, List<Object> arguments,
                                         MessagePackObjectMapper argumentsMapper,
                                         MessagePackValueMapper resultMapper) throws TarantoolClientException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean isClosed() {
        return !connected.get();
    }

    @Override
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        if (connected.compareAndSet(true, false)) {
            channel.pipeline().close();
            channel.closeFuture().syncUninterruptibly();
        }
    }
}
