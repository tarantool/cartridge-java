package io.tarantool.driver.api.space;

import io.tarantool.driver.CRUDTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.TarantoolResultMapperFactory;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import io.tarantool.driver.protocol.operations.TupleOperations;
import io.tarantool.driver.proxy.DeleteProxyOperation;
import io.tarantool.driver.proxy.InsertProxyOperation;
import io.tarantool.driver.proxy.ProxyOperation;
import io.tarantool.driver.proxy.ProxySelectArgumentsConverter;
import io.tarantool.driver.proxy.ReplaceProxyOperation;
import io.tarantool.driver.proxy.SelectProxyOperation;
import io.tarantool.driver.proxy.UpdateProxyOperation;
import io.tarantool.driver.proxy.UpsertProxyOperation;
import org.msgpack.value.ArrayValue;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @author Sergey Volgin
 */
public class ProxyTarantoolSpace implements TarantoolSpaceOperations {

    private final String spaceName;
    private final CRUDTarantoolClient client;
    private final TarantoolClientConfig config;
    private final TarantoolMetadataOperations operations;
    private final TarantoolIndexQueryFactory indexQueryFactory;

    private final TarantoolResultMapperFactory tarantoolResultMapperFactory;

    public ProxyTarantoolSpace(CRUDTarantoolClient client,
                               String spaceName,
                               TarantoolMetadataOperations operations) {
        this.spaceName = spaceName;
        this.client = client;
        this.config = client.getConfig();
        this.tarantoolResultMapperFactory = new TarantoolResultMapperFactory();
        //this.tarantoolResultMapperFactory.withConverter(getDefaultTarantoolTupleValueConverter());
        this.operations = operations;
        this.indexQueryFactory = new TarantoolIndexQueryFactory(operations);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(TarantoolIndexQuery indexQuery)
            throws TarantoolClientException {
        return delete(indexQuery, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(TarantoolIndexQuery indexQuery,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {

        return executeOperation(new DeleteProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getDeleteFunctionName())
                .withIndexQuery(indexQuery)
                .withValueConverter(tupleMapper)
                .build());
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> insert(TarantoolTuple tuple)
            throws TarantoolClientException {
        return insert(tuple, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> insert(TarantoolTuple tuple,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        InsertProxyOperation<T> operation = new InsertProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getInsertFunctionName())
                .withTuple(tuple)
                .withValueConverter(tupleMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> replace(TarantoolTuple tuple)
            throws TarantoolClientException {
        return replace(tuple, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> replace(TarantoolTuple tuple,
                                                             ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {
        ReplaceProxyOperation<T> operation = new ReplaceProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getReplaceFunctionName())
                .withTuple(tuple)
                .withValueConverter(tupleMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolSelectOptions options)
            throws TarantoolClientException {
        return select(indexQueryFactory.primary(), options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName, TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byId(indexName, spaceName);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(String indexName,
                                                                     TarantoolIteratorType iteratorType,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = indexQueryFactory.byId(indexName, spaceName)
                .withIteratorType(iteratorType);
        return select(indexQuery, options);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> select(TarantoolIndexQuery indexQuery,
                                                                     TarantoolSelectOptions options)
            throws TarantoolClientException {
        ValueConverter<ArrayValue, TarantoolTuple> converter = getDefaultTarantoolTupleValueConverter();
        return select(indexQuery, options, converter);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            Class<T> clazz) throws TarantoolClientException {
        Optional<ValueConverter<ArrayValue, T>> tupleMapper = tarantoolResultMapperFactory.getValueConverter(clazz);
        if (!tupleMapper.isPresent()) {
            throw new TarantoolClientException("Converter for class %s not found", clazz);
        }
        return select(indexQuery, options, tupleMapper.get());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(TarantoolIndexQuery indexQuery,
                                                            TarantoolSelectOptions options,
                                                            ValueConverter<ArrayValue, T> tupleMapper)
            throws TarantoolClientException {

        List<?> selectArguments = Collections.EMPTY_LIST;
        if (!indexQuery.getKeyValues().isEmpty()) {
            Optional<TarantoolSpaceMetadata> spaceMetadata = operations.getSpaceByName(spaceName);
            Optional<TarantoolIndexMetadata> indexMetadata =
                    operations.getIndexById(spaceName, indexQuery.getIndexId());

            if (!indexMetadata.isPresent() || !spaceMetadata.isPresent()) {
                throw new TarantoolClientException("Index metadata not found for index id: {}",
                        indexQuery.getIndexId());
            }

            selectArguments =
                    ProxySelectArgumentsConverter.fromIndexQuery(indexQuery,
                            indexMetadata.get().getIndexParts(),
                            spaceMetadata.get());
        }

        SelectProxyOperation<T> operation = new SelectProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getSelectFunctionName())
                .withSelectArguments(selectArguments)
                .withSelectOptions(options)
                .withValueConverter(tupleMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(TarantoolIndexQuery indexQuery,
                                                                     TupleOperations operations) {
        return update(indexQuery, operations, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> update(TarantoolIndexQuery indexQuery,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        UpdateProxyOperation<T> operation = new UpdateProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getUpdateFunctionName())
                .withIndexQuery(indexQuery)
                .withTupleOperation(operations)
                .withValueConverter(tupleMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> upsert(TarantoolIndexQuery indexQuery,
                                                                     TarantoolTuple tuple,
                                                                     TupleOperations operations) {
        return upsert(indexQuery, tuple, operations, getDefaultTarantoolTupleValueConverter());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> upsert(TarantoolIndexQuery indexQuery,
                                                            TarantoolTuple tuple,
                                                            TupleOperations operations,
                                                            ValueConverter<ArrayValue, T> tupleMapper) {
        UpsertProxyOperation<T> operation = new UpsertProxyOperation.Builder<T>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getUpsertFunctionName())
                .withTuple(tuple)
                .withTupleOperation(operations)
                .withValueConverter(tupleMapper)
                .build();

        return executeOperation(operation);
    }

    private <T> CompletableFuture<TarantoolResult<T>> executeOperation(ProxyOperation<T> operation) {
        return operation.execute();
    }

    private ValueConverter<ArrayValue, TarantoolTuple> getDefaultTarantoolTupleValueConverter() {
        return tarantoolResultMapperFactory.getDefaultTupleValueConverter(config.getMessagePackMapper());
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", spaceName);
    }
}
