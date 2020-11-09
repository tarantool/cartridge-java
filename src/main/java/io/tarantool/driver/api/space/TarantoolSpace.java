package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolSimpleResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.protocol.requests.TarantoolDeleteRequest;
import io.tarantool.driver.protocol.requests.TarantoolInsertRequest;
import io.tarantool.driver.protocol.requests.TarantoolReplaceRequest;
import io.tarantool.driver.protocol.requests.TarantoolSelectRequest;
import io.tarantool.driver.protocol.requests.TarantoolUpdateRequest;
import io.tarantool.driver.protocol.requests.TarantoolUpsertRequest;
import org.msgpack.value.ArrayValue;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Basic Tarantool space operations implementation for standalone server
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpace implements TarantoolSpaceOperations {

    private final int spaceId;
    private final TarantoolClientConfig config;
    private final TarantoolConnectionManager connectionManager;
    private final TarantoolSpaceMetadata spaceMetadata;
    private final TarantoolMetadataOperations metadataOperations;
    private final TarantoolSimpleResultMapperFactory tarantoolResultMapperFactory;

    /**
     * Basic constructor.
     * @param config client config
     * @param connectionManager Tarantool server connection manager
     * @param spaceMetadata metadata for this space
     * @param metadataOperations metadata operations implementation
     */
    public TarantoolSpace(TarantoolClientConfig config,
                          TarantoolConnectionManager connectionManager,
                          TarantoolSpaceMetadata spaceMetadata,
                          TarantoolMetadataOperations metadataOperations) {
        this.spaceId = spaceMetadata.getSpaceId();
        this.config = config;
        this.connectionManager = connectionManager;
        this.spaceMetadata = spaceMetadata;
        this.metadataOperations = metadataOperations;
        this.tarantoolResultMapperFactory = new TarantoolSimpleResultMapperFactory(config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(Conditions conditions)
            throws TarantoolClientException {
        return delete(conditions, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return delete(conditions, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                             MessagePackValueMapper resultMapper)
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple)
            throws TarantoolClientException {
        return insert(tuple, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return replace(tuple, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple,
                                                            MessagePackValueMapper resultMapper)
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple)
            throws TarantoolClientException {
        return replace(tuple, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple,
                                                             ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return replace(tuple, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple,
                                                             MessagePackValueMapper resultMapper)
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(Conditions conditions)
            throws TarantoolClientException {
        return select(conditions, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            Class<T> tupleClass)
            throws TarantoolClientException {
        MessagePackValueMapper mapper;
        if (TarantoolTuple.class.isAssignableFrom(tupleClass)) {
            mapper = defaultTupleResultMapper();
        } else {
            ValueConverter<ArrayValue, T> converter = getConverter(tupleClass);
            mapper = tarantoolResultMapperFactory.withConverter(tupleClass, converter);
        }
        return select(conditions, mapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return select(conditions, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                             MessagePackValueMapper resultMapper)
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions, TarantoolTuple tuple) {
        return update(conditions, TupleOperations.fromTarantoolTuple(tuple), defaultTupleResultMapper());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions,
                                                                     TupleOperations operations) {
        return update(conditions, operations, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> update(Conditions conditions,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return update(conditions, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> update(Conditions conditions,
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

            TarantoolUpdateRequest request = new TarantoolUpdateRequest.Builder(spaceMetadata)
                    .withSpaceId(spaceId)
                    .withIndexId(indexQuery.getIndexId())
                    .withKeyValues(indexQuery.getKeyValues())
                    .withTupleOperations(operations)
                    .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(Conditions conditions,
                                                                     TarantoolTuple tuple,
                                                                     TupleOperations operations) {
        return upsert(conditions, tuple, operations, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> upsert(Conditions conditions,
                                                            TarantoolTuple tuple,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return upsert(conditions, tuple, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> upsert(Conditions conditions,
                                                             TarantoolTuple tuple,
                                                             TupleOperations operations,
                                                             MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
            TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

            TarantoolUpsertRequest request = new TarantoolUpsertRequest.Builder(spaceMetadata)
                    .withSpaceId(spaceId)
                    .withKeyValues(indexQuery.getKeyValues())
                    .withTuple(tuple)
                    .withTupleOperations(operations)
                    .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    private <T> ValueConverter<ArrayValue, T> getConverter(Class<T> tupleClass) {
        Optional<ValueConverter<ArrayValue, T>> converter =
                config.getMessagePackMapper().getValueConverter(ArrayValue.class, tupleClass);
        if (!converter.isPresent()) {
            throw new TarantoolClientException("No ArrayValue converter for type " + tupleClass + " is present");
        }
        return converter.get();
    }

    private MessagePackValueMapper defaultTupleResultMapper() {
        return tarantoolResultMapperFactory.withDefaultTupleValueConverter(spaceMetadata);
    }

    private <T> CompletableFuture<TarantoolResult<T>> sendRequest(TarantoolRequest request,
                                                                  MessagePackValueMapper resultMapper) {
        try {
            return connectionManager.getConnection().sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public String toString() {
        return String.format("TarantoolSpace %s [%d]", spaceMetadata.getSpaceName(), spaceMetadata.getSpaceId());
    }
}
