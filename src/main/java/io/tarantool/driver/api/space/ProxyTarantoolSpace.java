package io.tarantool.driver.api.space;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.ProxyTarantoolBatchCursor;
import io.tarantool.driver.api.cursor.TarantoolBatchCursorOptions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.protocol.operations.TupleOperations;
import io.tarantool.driver.proxy.DeleteProxyOperation;
import io.tarantool.driver.proxy.InsertProxyOperation;
import io.tarantool.driver.proxy.ProxyOperation;
import io.tarantool.driver.proxy.ReplaceProxyOperation;
import io.tarantool.driver.proxy.SelectProxyOperation;
import io.tarantool.driver.proxy.UpdateProxyOperation;
import io.tarantool.driver.proxy.UpsertProxyOperation;
import org.msgpack.value.ArrayValue;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a proxy {@link TarantoolSpaceOperations} implementation, which uses calls to API functions defined in
 * Tarantool instance for performing CRUD operations on a space
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolSpace implements TarantoolSpaceOperations {

    private final String spaceName;
    private final ProxyTarantoolClient client;
    private final TarantoolMetadataOperations metadataOperations;
    private final TarantoolSpaceMetadata spaceMetadata;

    private final TarantoolCallResultMapperFactory tarantoolResultMapperFactory;

    public ProxyTarantoolSpace(ProxyTarantoolClient client,
                               TarantoolSpaceMetadata spaceMetadata) {
        this.client = client;
        this.spaceMetadata = spaceMetadata;
        this.spaceName = spaceMetadata.getSpaceName();
        this.metadataOperations = client.metadata();
        this.tarantoolResultMapperFactory =
                new TarantoolCallResultMapperFactory(client.getConfig().getMessagePackMapper());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(Conditions conditions)
            throws TarantoolClientException {
        return delete(conditions, tarantoolResultMapperFactory.getByClass(TarantoolTuple.class));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {

        return delete(conditions, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                            TarantoolCallResultMapper<T> resultMapper)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        DeleteProxyOperation<T> operation = new DeleteProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getDeleteFunctionName())
                .withIndexQuery(indexQuery)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
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
        return insert(tuple, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple,
                                                            TarantoolCallResultMapper<T> resultMapper)
            throws TarantoolClientException {
        InsertProxyOperation<T> operation = new InsertProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getInsertFunctionName())
                .withTuple(tuple)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
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
                                                             TarantoolCallResultMapper<T> resultMapper)
            throws TarantoolClientException {
        ReplaceProxyOperation<T> operation = new ReplaceProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getReplaceFunctionName())
                .withTuple(tuple)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(Conditions conditions)
            throws TarantoolClientException {
        return select(conditions, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            Class<T> tupleClass) throws TarantoolClientException {
        return select(conditions, options, tarantoolResultMapperFactory.getByClass(tupleClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return select(conditions, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    @Override
    @SuppressWarnings("unchecked")
    public  <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                             TarantoolSelectOptions options,
                                                             MessagePackValueMapper resultMapper)
            throws TarantoolClientException {

        SelectProxyOperation<T> operation = new SelectProxyOperation.Builder<T>(metadataOperations, spaceMetadata)
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getSelectFunctionName())
                .withConditions(conditions)
                .withResultMapper((TarantoolCallResultMapper<T>) resultMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(TarantoolIndexQuery indexQuery) throws TarantoolClientException {
        return cursor(indexQuery, new TarantoolBatchCursorOptions());
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(TarantoolIndexQuery indexQuery, TarantoolBatchCursorOptions options)
            throws TarantoolClientException {
        return cursor(indexQuery, options, defaultTupleResultMapper());
    }

    @Override
    public <T> TarantoolCursor<T> cursor(TarantoolIndexQuery indexQuery,
                                  TarantoolBatchCursorOptions options,
                                  ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return cursor(indexQuery, options, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> TarantoolCursor<T> cursor(TarantoolIndexQuery indexQuery,
                                          TarantoolBatchCursorOptions options,
                                          TarantoolCallResultMapper<T> resultMapper) throws TarantoolClientException {
        Optional<TarantoolSpaceMetadata> spaceMetadata = metadataOperations.getSpaceByName(spaceName);
        if (!spaceMetadata.isPresent()) {
            throw new TarantoolClientException("Space metadata not found for : {}", spaceName);
        }

        return new ProxyTarantoolBatchCursor<T>(this, indexQuery, options, resultMapper, spaceMetadata.get());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions,
                                                                     TupleOperations operations) {
        return update(conditions, operations, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> update(Conditions conditions,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        return update(conditions, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> update(Conditions conditions,
                                                             TupleOperations operations,
                                                             TarantoolCallResultMapper<T> resultMapper) {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        UpdateProxyOperation<T> operation = new UpdateProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getUpdateFunctionName())
                .withIndexQuery(indexQuery)
                .withTupleOperation(operations)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
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
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        return upsert(conditions, tuple, operations, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> CompletableFuture<TarantoolResult<T>> upsert(Conditions conditions,
                                                             TarantoolTuple tuple,
                                                             TupleOperations operations,
                                                             TarantoolCallResultMapper<T> resultMapper) {
        UpsertProxyOperation<T> operation = new UpsertProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getUpsertFunctionName())
                .withTuple(tuple)
                .withTupleOperation(operations)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
    }

    private TarantoolCallResultMapper<TarantoolTuple> defaultTupleResultMapper() {
        this.tarantoolResultMapperFactory.withDefaultTupleValueConverter(spaceMetadata);
        return tarantoolResultMapperFactory.getByClass(TarantoolTuple.class);
    }

    private <T> CompletableFuture<TarantoolResult<T>> executeOperation(ProxyOperation<T> operation) {
        return operation.execute();
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", spaceName);
    }
}
