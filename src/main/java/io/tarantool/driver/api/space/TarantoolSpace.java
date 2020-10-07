package io.tarantool.driver.api.space;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.core.TarantoolConnectionManager;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolSimpleResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.TarantoolProtocolException;
import io.tarantool.driver.protocol.TarantoolRequest;
import io.tarantool.driver.protocol.operations.TupleOperations;
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
    private final TarantoolIndexQueryFactory indexQueryFactory;
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
        this.indexQueryFactory = new TarantoolIndexQueryFactory(metadataOperations);
        this.metadataOperations = metadataOperations;
        this.tarantoolResultMapperFactory = new TarantoolSimpleResultMapperFactory(config.getMessagePackMapper());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(TarantoolIndexQuery indexQuery)
            throws TarantoolClientException {
        return delete(indexQuery, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return delete(indexQuery, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery,
                                                             MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolSelectOptions options)
            throws TarantoolClientException {
        return select(indexQueryFactory.primary(), options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(spaceId, indexName);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolIteratorType iteratorType,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byName(spaceId, indexName).withIteratorType(iteratorType);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolIndexQuery indexQuery,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        return select(indexQuery, options, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            Class<T> tupleClass)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, T> converter = getConverter(tupleClass);
        return select(indexQuery, options, tarantoolResultMapperFactory.withConverter(tupleClass, converter));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return select(indexQuery, options, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
            TarantoolSelectRequest request = new TarantoolSelectRequest.Builder()
                    .withSpaceId(spaceId)
                    .withIndexId(indexQuery.getIndexId())
                    .withIteratorType(indexQuery.getIteratorType())
                    .withKeyValues(indexQuery.getKeyValues())
                    .withLimit(options.getLimit())
                    .withOffset(options.getOffset())
                    .build(config.getMessagePackMapper());

            return sendRequest(request, resultMapper);
        } catch (TarantoolProtocolException e) {
            throw new TarantoolClientException(e);
        }
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(TarantoolIndexQuery indexQuery,
                                                                     TupleOperations operations) {
        return update(indexQuery, operations, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> update(TarantoolIndexQuery indexQuery,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return update(indexQuery, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> update(TarantoolIndexQuery indexQuery,
                                                             TupleOperations operations,
                                                             MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
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
    public CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(TarantoolIndexQuery indexQuery,
                                                                     TarantoolTuple tuple,
                                                                     TupleOperations operations) {
        TarantoolSpaceMetadata meta = metadataOperations.getSpaceById(spaceId).orElse(null);
        return upsert(indexQuery, tuple, operations, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> upsert(TarantoolIndexQuery indexQuery,
                                                            TarantoolTuple tuple,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return upsert(indexQuery, tuple, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> upsert(TarantoolIndexQuery indexQuery,
                                                             TarantoolTuple tuple,
                                                             TupleOperations operations,
                                                             MessagePackValueMapper resultMapper)
            throws TarantoolClientException {
        try {
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
