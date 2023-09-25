package io.tarantool.driver.core.space;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolVoidResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.options.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.operations.TupleOperation;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.connection.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.requests.TarantoolCallRequest;
import io.tarantool.driver.protocol.requests.TarantoolDeleteRequest;
import io.tarantool.driver.protocol.requests.TarantoolInsertRequest;
import io.tarantool.driver.protocol.requests.TarantoolReplaceRequest;
import io.tarantool.driver.protocol.requests.TarantoolSelectRequest;
import io.tarantool.driver.protocol.requests.TarantoolUpdateRequest;
import io.tarantool.driver.protocol.requests.TarantoolUpsertRequest;
import org.msgpack.value.ArrayValue;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Basic implementation for working with spaces via Tarantool protocol requests
 *
 * @author Alexey Kuzin
 */
public abstract class TarantoolSpace<T extends Packable, R extends Collection<T>>
    implements TarantoolSpaceOperations<T, R> {

    private final int spaceId;
    private final TarantoolClientConfig config;
    private final TarantoolConnectionManager connectionManager;
    private final TarantoolSpaceMetadata spaceMetadata;
    private final TarantoolMetadataOperations metadataOperations;

    public TarantoolSpace(
        TarantoolClientConfig config,
        TarantoolConnectionManager connectionManager,
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata) {
        this.spaceId = spaceMetadata.getSpaceId();
        this.config = config;
        this.connectionManager = connectionManager;
        this.spaceMetadata = spaceMetadata;
        this.metadataOperations = metadataOperations;
    }

    @Override
    public CompletableFuture<R> delete(Conditions conditions) throws TarantoolClientException {
        return delete(conditions, arrayTupleResultMapper());
    }

    private CompletableFuture<R> delete(Conditions conditions, MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

            TarantoolDeleteRequest request = new TarantoolDeleteRequest.Builder()
                .withSpaceId(spaceId)
                .withIndexId(indexQuery.getIndexId())
                .withKeyValues(indexQuery.getKeyValues())
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<R> insert(T tuple) throws TarantoolClientException {
        return insert(tuple, arrayTupleResultMapper());
    }

    @Override
    public CompletableFuture<R> insertMany(Collection<T> tuples) throws TarantoolClientException {
        // TODO: add support with interactive transactions if the node has the MVCC mode enabled
        // TODO: add support in all other cases if https://github.com/tarantool/tarantool/issues/7691 is implemented
        throw new UnsupportedOperationException(
            "Standalone node API does not support inserting several tuples at once yet");
    }

    private CompletableFuture<R> insert(T tuple, MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolInsertRequest request = new TarantoolInsertRequest.Builder()
                .withSpaceId(spaceId)
                .withTuple(tuple)
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<R> replace(T tuple) throws TarantoolClientException {
        return replace(tuple, arrayTupleResultMapper());
    }

    @Override
    public CompletableFuture<R> replaceMany(Collection<T> tuples) throws TarantoolClientException {
        // TODO: add support with interactive transactions if the node has the MVCC mode enabled
        // TODO: add support in all other cases if https://github.com/tarantool/tarantool/issues/7691 is implemented
        throw new UnsupportedOperationException(
            "Standalone node API does not support replacing several tuples at once yet");
    }

    private CompletableFuture<R> replace(T tuple, MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolReplaceRequest request = new TarantoolReplaceRequest.Builder()
                .withSpaceId(spaceId)
                .withTuple(tuple)
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<R> select(Conditions conditions) throws TarantoolClientException {
        return select(conditions, arrayTupleResultMapper());
    }

    private CompletableFuture<R> select(Conditions conditions, MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);
            TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
                .withSpaceId(spaceId)
                .withIndexId(indexQuery.getIndexId())
                .withIteratorType(indexQuery.getIteratorType())
                .withKeyValues(indexQuery.getKeyValues())
                .withLimit(conditions.getLimit())
                .withOffset(conditions.getOffset())
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, T tuple) {
        return update(conditions, makeOperationsFromTuple(tuple), arrayTupleResultMapper());
    }

    /**
     * Create a {@link TupleOperations} instance from the given tuple of type {@code T}
     *
     * @param tuple tuple of the specified type
     * @return new {@link TupleOperations} instance
     */
    protected abstract TupleOperations makeOperationsFromTuple(T tuple);

    @Override
    public CompletableFuture<R> update(Conditions conditions, TupleOperations operations) {
        return update(conditions, operations, arrayTupleResultMapper());
    }

    private CompletableFuture<R> update(
        Conditions conditions,
        TupleOperations operations,
        MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

            Optional<TarantoolIndexMetadata> indexMetadata =
                metadataOperations.getIndexById(spaceId, indexQuery.getIndexId());

            if (!indexMetadata.isPresent() || !indexMetadata.get().isUnique()) {
                throw new TarantoolSpaceOperationException("Index must be primary or unique for update operation");
            }

            TarantoolUpdateRequest request = new TarantoolUpdateRequest.Builder()
                .withSpaceId(spaceId)
                .withIndexId(indexQuery.getIndexId())
                .withKeyValues(indexQuery.getKeyValues())
                .withTupleOperations(fillFieldIndexFromMetadata(operations))
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations) {
        return upsert(conditions, tuple, operations, arrayTupleResultMapper());
    }

    private CompletableFuture<R> upsert(
        Conditions conditions,
        T tuple,
        TupleOperations operations,
        MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

            TarantoolUpsertRequest request = new TarantoolUpsertRequest.Builder()
                .withSpaceId(spaceId)
                .withKeyValues(indexQuery.getKeyValues())
                .withTuple(tuple)
                .withTupleOperations(fillFieldIndexFromMetadata(operations))
                .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> truncate() throws TarantoolClientException {
        return truncate(arrayTupleResultMapper());
    }

    private CompletableFuture<Void> truncate(MessagePackValueMapper resultMapper)
        throws TarantoolClientException {
        try {
            String spaceName = spaceMetadata.getSpaceName();
            TarantoolCallRequest request = new TarantoolCallRequest.Builder()
                .withFunctionName("box.space." + spaceName + ":truncate")
                .build(config.getMessagePackMapper());
            return sendRequest(request, resultMapper)
                .thenApply(v -> TarantoolVoidResult.INSTANCE.value());
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * MessagePack value mapper configured with an ArrayValue to tuple converter corresponding to the selected
     * tuple type
     *
     * @return configured mapper with {@link ArrayValue} to {@code T} converter
     */
    protected abstract MessagePackValueMapper arrayTupleResultMapper();

    private CompletableFuture<R> sendRequest(TarantoolRequest request, MessagePackValueMapper resultMapper) {
        return connectionManager.getConnection().thenCompose(c -> c.sendRequest(request, resultMapper));
    }

    @Override
    public TarantoolSpaceMetadata getMetadata() {
        return spaceMetadata;
    }

    @Override
    public String toString() {
        return String.format(
            "StandaloneTarantoolSpace %s [%d]", spaceMetadata.getSpaceName(), spaceMetadata.getSpaceId());
    }

    private List<TupleOperation> fillFieldIndexFromMetadata(TupleOperations operations) {
        return operations.asList().stream()
            .map(operation -> {
                if (operation.getFieldIndex() == null) {
                    String fieldName = operation.getFieldName();
                    int fieldMetadataIndex = this.spaceMetadata.getFieldByName(fieldName)
                        .orElseThrow(() -> new TarantoolSpaceFieldNotFoundException(fieldName))
                        .getFieldPosition();
                    return operation.cloneWithIndex(fieldMetadataIndex);
                }
                return operation;
            }).collect(Collectors.toList());
    }
}
