package io.tarantool.driver.api.space;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.mappers.DefaultResultMapperFactoryFactory;
import io.tarantool.driver.mappers.DefaultTarantoolTupleValueConverter;
import io.tarantool.driver.mappers.SingleValueTarantoolResultMapperFactory;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
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

    private final DefaultResultMapperFactoryFactory mapperFactoryFactory;

    public ProxyTarantoolSpace(ProxyTarantoolClient client,
                               TarantoolSpaceMetadata spaceMetadata) {
        this.client = client;
        this.spaceMetadata = spaceMetadata;
        this.spaceName = spaceMetadata.getSpaceName();
        this.metadataOperations = client.metadata();
        this.mapperFactoryFactory = new DefaultResultMapperFactoryFactory();
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> delete(Conditions conditions)
            throws TarantoolClientException {
        return delete(conditions, defaultTupleResultMapper());
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> delete(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass)
            throws TarantoolClientException {

        return delete(conditions, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<TarantoolResult<T>> delete(
            Conditions conditions,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        DeleteProxyOperation<TarantoolResult<T>> operation = new DeleteProxyOperation.Builder<TarantoolResult<T>>()
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
                                                            ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass)
            throws TarantoolClientException {
        return insert(tuple, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<TarantoolResult<T>> insert(
            TarantoolTuple tuple,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        InsertProxyOperation<TarantoolResult<T>> operation = new InsertProxyOperation.Builder<TarantoolResult<T>>()
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
                                                             ValueConverter<ArrayValue, T> tupleConverter,
                                                             Class<? extends TarantoolResult<T>> resultClass)
            throws TarantoolClientException {
        return replace(tuple, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<TarantoolResult<T>> replace(
            TarantoolTuple tuple,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper)
            throws TarantoolClientException {
        ReplaceProxyOperation<TarantoolResult<T>> operation = new ReplaceProxyOperation.Builder<TarantoolResult<T>>()
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
        CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> mapper =
                mapperFactoryFactory.singleValueTarantoolResultMapperFactory(tupleClass)
                .withTarantoolResultConverter(getConverter(tupleClass));
        return select(conditions, mapper);
    }

    @Override
    public <T> CompletableFuture<TarantoolResult<T>> select(Conditions conditions,
                                                            ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass)
            throws TarantoolClientException {
        return select(conditions, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<T> select(
            Conditions conditions,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper)
            throws TarantoolClientException {

        SelectProxyOperation<T> operation = new SelectProxyOperation.Builder<T>(metadataOperations, spaceMetadata)
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getSelectFunctionName())
                .withConditions(conditions)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<TarantoolResult<TarantoolTuple>> update(Conditions conditions,
                                                                     TarantoolTuple tuple) {
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
                                                            ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass) {
        return update(conditions, operations, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<TarantoolResult<T>> update(
            Conditions conditions,
            TupleOperations operations,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper) {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        UpdateProxyOperation<TarantoolResult<T>> operation = new UpdateProxyOperation.Builder<TarantoolResult<T>>()
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
                                                            ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass) {
        return upsert(conditions, tuple, operations, withTupleConverter(tupleConverter, resultClass));
    }

    private <T> CompletableFuture<TarantoolResult<T>> upsert(
            Conditions conditions,
            TarantoolTuple tuple,
            TupleOperations operations,
            CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>> resultMapper) {

        UpsertProxyOperation<TarantoolResult<T>> operation = new UpsertProxyOperation.Builder<TarantoolResult<T>>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(client.getUpsertFunctionName())
                .withTuple(tuple)
                .withTupleOperation(operations)
                .withResultMapper(resultMapper)
                .build();

        return executeOperation(operation);
    }

    @SuppressWarnings("unchecked")
    private
    <T> CallResultMapper<TarantoolResult<T>, SingleValueCallResult<TarantoolResult<T>>>
    withTupleConverter(ValueConverter<ArrayValue, T> tupleConverter, Class<? extends TarantoolResult<T>> resultClass) {
        return ((SingleValueTarantoolResultMapperFactory<T>)
                mapperFactoryFactory.singleValueTarantoolResultMapperFactory())
                .withTarantoolResultConverter(tupleConverter);
    }

    private CallResultMapper<TarantoolResult<TarantoolTuple>, SingleValueCallResult<TarantoolResult<TarantoolTuple>>>
    defaultTupleResultMapper() {
        return mapperFactoryFactory.defaultTupleSingleResultMapperFactory()
                .withDefaultTupleValueConverter(client.getConfig().getMessagePackMapper(), spaceMetadata);
    }

    @SuppressWarnings("unchecked")
    private <T> ValueConverter<ArrayValue, T> getConverter(Class<T> tupleClass) {
        if (TarantoolTuple.class.isAssignableFrom(tupleClass)) {
            return (ValueConverter<ArrayValue, T>)
                    new DefaultTarantoolTupleValueConverter(client.getConfig().getMessagePackMapper(), spaceMetadata);
        } else {
            Optional<ValueConverter<ArrayValue, T>> converter =
                    client.getConfig().getMessagePackMapper().getValueConverter(ArrayValue.class, tupleClass);
            if (!converter.isPresent()) {
                throw new TarantoolClientException("No ArrayValue converter for type " + tupleClass + " is present");
            }
            return converter.get();
        }
    }

    private <T> CompletableFuture<T> executeOperation(ProxyOperation<T> operation) {
        return operation.execute();
    }

    @Override
    public TarantoolSpaceMetadata getMetadata() {
        return spaceMetadata;
    }

    @Override
    public String toString() {
        return String.format("ProxyTarantoolSpace [%s]", spaceName);
    }
}
