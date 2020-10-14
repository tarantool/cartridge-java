package io.tarantool.driver.api.space;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.ProxyTarantoolBatchCursor;
import io.tarantool.driver.api.cursor.TarantoolCursorOptions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.mappers.AbstractTarantoolResultMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.mappers.TarantoolCallResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadataOperations;
import io.tarantool.driver.protocol.operations.TupleOperations;
import io.tarantool.driver.proxy.DeleteProxyOperation;
import io.tarantool.driver.proxy.InsertProxyOperation;
import io.tarantool.driver.proxy.ProxyOperation;
import io.tarantool.driver.proxy.ReplaceProxyOperation;
import io.tarantool.driver.proxy.SelectProxyOperation;
import io.tarantool.driver.proxy.UpdateProxyOperation;
import io.tarantool.driver.proxy.UpsertProxyOperation;
import org.msgpack.value.ArrayValue;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a proxy {@link TarantoolSpaceOperations} implementation, which uses calls to API functions defined in
 * Tarantool instance for performing CRUD operations on a space
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolSpace implements TarantoolSpaceOperations, TarantoolSpaceMetadataOperations {

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
        return delete(conditions, defaultTupleResultMapper());
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
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(this);

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
                                                            Class<T> tupleClass)
            throws TarantoolClientException {
        return select(conditions, tarantoolResultMapperFactory.getByClass(tupleClass));
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return select(conditions, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    @Override
    public  <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                             AbstractTarantoolResultMapper<T> resultMapper)
            throws TarantoolClientException {

        SelectProxyOperation<T> operation = new SelectProxyOperation.Builder<T>(this)
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getSelectFunctionName())
                .withConditions(conditions)
                .withResultMapper((TarantoolCallResultMapper<T>) resultMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions) throws TarantoolClientException {
        return cursor(conditions, new TarantoolCursorOptions());
    }

    @Override
    public TarantoolCursor<TarantoolTuple> cursor(Conditions conditions, TarantoolCursorOptions options)
            throws TarantoolClientException {
        return cursor(conditions, options, defaultTupleResultMapper());
    }

    @Override
    public <T> TarantoolCursor<T> cursor(Conditions conditions,
                                         TarantoolCursorOptions options,
                                         ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        return cursor(conditions, options, tarantoolResultMapperFactory.withConverter(tupleMapper));
    }

    private <T> TarantoolCursor<T> cursor(Conditions conditions,
                                          TarantoolCursorOptions options,
                                          TarantoolCallResultMapper<T> resultMapper) throws TarantoolClientException {
        return new ProxyTarantoolBatchCursor<>(this, conditions, options, resultMapper);
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
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(this);

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

    @Override
    public TarantoolIndexMetadata getIndexById(int indexId) {
        Optional<TarantoolIndexMetadata> indexMetadata = metadataOperations.getIndexById(spaceName, indexId);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceMetadata.getSpaceName(), indexId);
        }

        return indexMetadata.get();
    }

    @Override
    public TarantoolIndexMetadata getIndexByName(String indexName) {
        Optional<TarantoolIndexMetadata> indexMetadata = metadataOperations.getIndexByName(spaceName, indexName);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceName, indexName);
        }
        return indexMetadata.get();
    }

    @Override
    public Map<String, TarantoolIndexMetadata> getSpaceIndexes() {
        Optional<Map<String, TarantoolIndexMetadata>> indexMetadataMap = metadataOperations.getSpaceIndexes(spaceName);
        if (!indexMetadataMap.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceName);
        }
        return indexMetadataMap.get();
    }

    @Override
    public TarantoolSpaceMetadata getSpaceMetadata() {
        return spaceMetadata;
    }

    private TarantoolCallResultMapper<TarantoolTuple> defaultTupleResultMapper() {
        return this.tarantoolResultMapperFactory.withDefaultTupleValueConverter(spaceMetadata);
    }

    private <T> CompletableFuture<TarantoolResult<T>> executeOperation(ProxyOperation<T> operation) {
        return operation.execute();
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", spaceName);
    }
}
