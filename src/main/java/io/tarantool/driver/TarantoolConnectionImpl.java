package io.tarantool.driver;

import io.netty.channel.Channel;
import io.tarantool.driver.core.RequestFutureManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceNotFoundException;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpace;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import org.springframework.util.Assert;

import java.util.Collections;
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
    public CompletableFuture<List<Object>> call(String functionName) throws TarantoolClientException {
        return call(functionName, Collections.emptyList());
    }

    @Override
    public CompletableFuture<List<Object>> call(String functionName, List<Object> arguments)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper());
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments,
                                                               MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        return call(functionName, arguments, config.getMessagePackMapper(), resultMapper);
    }

    @Override
    public <T> CompletableFuture<List<T>> call(String functionName, List<Object> arguments,
                                         MessagePackObjectMapper argumentsMapper,
                                         MessagePackValueMapper resultMapper) throws TarantoolClientException {
        try {
            TarantoolCallRequest.Builder builder = new TarantoolCallRequest.Builder()
                    .withFunctionName(functionName);

            if (arguments.size() > 0) {
                builder.withArguments(arguments);
            }

            TarantoolCallRequest request = builder.build(argumentsMapper);

            CompletableFuture<List<T>> requestFuture = requestManager.submitRequest(request, resultMapper);

            getChannel().writeAndFlush(request).addListener(f -> {
                if (!f.isSuccess()) {
                    requestFuture.completeExceptionally(
                            new RuntimeException("Failed to send the request to Tarantool server", f.cause()));
                }
            });

            return requestFuture;
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
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
