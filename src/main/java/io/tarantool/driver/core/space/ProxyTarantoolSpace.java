package io.tarantool.driver.core.space;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.DeleteOptions;
import io.tarantool.driver.api.space.options.InsertManyOptions;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.ReplaceManyOptions;
import io.tarantool.driver.api.space.options.ReplaceOptions;
import io.tarantool.driver.api.space.options.SelectOptions;
import io.tarantool.driver.api.space.options.UpdateOptions;
import io.tarantool.driver.api.space.options.UpsertOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyDeleteOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyInsertManyOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyInsertOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyReplaceManyOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyReplaceOptions;
import io.tarantool.driver.api.space.options.proxy.ProxySelectOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyTruncateOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyUpdateOptions;
import io.tarantool.driver.api.space.options.proxy.ProxyUpsertOptions;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.proxy.DeleteProxyOperation;
import io.tarantool.driver.core.proxy.InsertProxyOperation;
import io.tarantool.driver.core.proxy.InsertManyProxyOperation;
import io.tarantool.driver.core.proxy.ProxyOperation;
import io.tarantool.driver.core.proxy.ReplaceProxyOperation;
import io.tarantool.driver.core.proxy.ReplaceManyProxyOperation;
import io.tarantool.driver.core.proxy.SelectProxyOperation;
import io.tarantool.driver.core.proxy.TruncateProxyOperation;
import io.tarantool.driver.core.proxy.UpdateProxyOperation;
import io.tarantool.driver.core.proxy.UpsertProxyOperation;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.protocol.Packable;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import org.msgpack.value.ArrayValue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Basic proxy {@link TarantoolSpaceOperations} implementation, which uses calls to API functions defined in
 * Tarantool instance for performing CRUD operations on a space
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public abstract class ProxyTarantoolSpace<T extends Packable, R extends Collection<T>>
        implements TarantoolSpaceOperations<T, R> {

    private final String spaceName;
    private final TarantoolClientConfig config;
    private final TarantoolCallOperations client;
    private final TarantoolMetadataOperations metadataOperations;
    private final ProxyOperationsMappingConfig operationsMapping;
    private final TarantoolSpaceMetadata spaceMetadata;

    public ProxyTarantoolSpace(TarantoolClientConfig config,
                               TarantoolCallOperations client,
                               ProxyOperationsMappingConfig operationsMapping,
                               TarantoolMetadataOperations metadata,
                               TarantoolSpaceMetadata spaceMetadata) {
        this.config = config;
        this.client = client;
        this.operationsMapping = operationsMapping;
        this.metadataOperations = metadata;
        this.spaceMetadata = spaceMetadata;
        this.spaceName = spaceMetadata.getSpaceName();
    }

    @Override
    public CompletableFuture<R> delete(Conditions conditions) throws TarantoolClientException {
        return delete(conditions, tupleResultMapper(), ProxyDeleteOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> delete(Conditions conditions, DeleteOptions options) throws TarantoolClientException {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return delete(conditions, tupleResultMapper(), options);
    }

    private CompletableFuture<R> delete(Conditions conditions,
                                        CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                        DeleteOptions options)
            throws TarantoolClientException {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        DeleteProxyOperation<R> operation = new DeleteProxyOperation.Builder<R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getDeleteFunctionName())
                .withIndexQuery(indexQuery)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> insert(T tuple) throws TarantoolClientException {
        return insert(tuple, tupleResultMapper(), ProxyInsertOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> insert(T tuple, InsertOptions options) throws TarantoolClientException {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return insert(tuple, tupleResultMapper(), options);
    }

    private CompletableFuture<R> insert(T tuple,
                                        CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                        InsertOptions options)
            throws TarantoolClientException {
        InsertProxyOperation<T, R> operation = new InsertProxyOperation.Builder<T, R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getInsertFunctionName())
                .withTuple(tuple)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> insertMany(Collection<T> tuples) {
        return insertMany(tuples, tupleResultMapper(), ProxyInsertManyOptions.create()
            .withTimeout(config.getRequestTimeout())
            .withStopOnError(true)
            .withRollbackOnError(true)
        );
    }

    @Override
    public CompletableFuture<R> insertMany(Collection<T> tuples, InsertManyOptions options)
            throws TarantoolClientException {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return insertMany(tuples, tupleResultMapper(), options);
    }

    private CompletableFuture<R> insertMany(Collection<T> tuples,
                                            CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                            InsertManyOptions options)
            throws TarantoolClientException {
        InsertManyProxyOperation<T, R> operation = new InsertManyProxyOperation.Builder<T, R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getInsertManyFunctionName())
                .withTuples(tuples)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> replace(T tuple) throws TarantoolClientException {
        return replace(tuple, tupleResultMapper(), ProxyReplaceOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> replace(T tuple, ReplaceOptions options) throws TarantoolClientException {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return replace(tuple, tupleResultMapper(), options);
    }

    private CompletableFuture<R> replace(T tuple,
                                         CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                         ReplaceOptions options)
            throws TarantoolClientException {
        ReplaceProxyOperation<T, R> operation = new ReplaceProxyOperation.Builder<T, R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getReplaceFunctionName())
                .withTuple(tuple)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> replaceMany(Collection<T> tuples) throws TarantoolClientException {
        return replaceMany(tuples, tupleResultMapper(), ProxyReplaceManyOptions.create()
            .withTimeout(config.getRequestTimeout())
            .withStopOnError(true)
            .withRollbackOnError(true)
        );
    }

    @Override
    public CompletableFuture<R> replaceMany(Collection<T> tuples, ReplaceManyOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return replaceMany(tuples, tupleResultMapper(), options);
    }

    private CompletableFuture<R> replaceMany(Collection<T> tuples,
                                             CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                             ReplaceManyOptions options)
            throws TarantoolClientException {
        ReplaceManyProxyOperation<T, R> operation = new ReplaceManyProxyOperation.Builder<T, R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getReplaceManyFunctionName())
                .withTuples(tuples)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> select(Conditions conditions) throws TarantoolClientException {
        return select(conditions, tupleResultMapper(), ProxySelectOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> select(Conditions conditions,
                                       SelectOptions options) throws TarantoolClientException {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return select(conditions, tupleResultMapper(), options);
    }

    private CompletableFuture<R> select(Conditions conditions,
                                        CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                        SelectOptions options)
            throws TarantoolClientException {
        SelectProxyOperation<R> operation = new SelectProxyOperation.Builder<R>(metadataOperations, spaceMetadata)
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getSelectFunctionName())
                .withConditions(conditions)
                .withOptions(options)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, T tuple) {
        return update(conditions, makeOperationsFromTuple(tuple), tupleResultMapper(), ProxyUpdateOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, T tuple, UpdateOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return update(conditions, makeOperationsFromTuple(tuple), tupleResultMapper(), options);
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
        return update(conditions, operations, tupleResultMapper(), ProxyUpdateOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, TupleOperations operations, UpdateOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return update(conditions, operations, tupleResultMapper(), options);
    }

    private CompletableFuture<R> update(Conditions conditions,
                                        TupleOperations operations,
                                        CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                        UpdateOptions options) {
        TarantoolIndexQuery indexQuery = conditions.toIndexQuery(metadataOperations, spaceMetadata);

        UpdateProxyOperation<R> operation = new UpdateProxyOperation.Builder<R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getUpdateFunctionName())
                .withIndexQuery(indexQuery)
                .withTupleOperation(operations)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations) {
        return upsert(conditions, tuple, operations, tupleResultMapper(), ProxyUpsertOptions.create()
            .withTimeout(config.getRequestTimeout())
        );
    }

    @Override
    public CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations,
                                       UpsertOptions options) {
        if (options == null) {
            throw new IllegalArgumentException("Options should not be null");
        }
        return upsert(conditions, tuple, operations, tupleResultMapper(), options);
    }

    private CompletableFuture<R> upsert(Conditions conditions,
                                        T tuple,
                                        TupleOperations operations,
                                        CallResultMapper<R, SingleValueCallResult<R>> resultMapper,
                                        UpsertOptions options) {

        UpsertProxyOperation<T, R> operation = new UpsertProxyOperation.Builder<T, R>()
                .withClient(client)
                .withSpaceName(spaceName)
                .withFunctionName(operationsMapping.getUpsertFunctionName())
                .withTuple(tuple)
                .withTupleOperation(operations)
                .withArgumentsMapper(config.getMessagePackMapper())
                .withResultMapper(resultMapper)
                .withOptions(options)
                .build();

        return executeOperation(operation);
    }

    @Override
    public CompletableFuture<Void> truncate() throws TarantoolClientException {
        try {
            return executeVoidOperation(TruncateProxyOperation.<Void>builder()
                    .withClient(client)
                    .withSpaceName(spaceName)
                    .withFunctionName(operationsMapping.getTruncateFunctionName())
                    .withOptions(ProxyTruncateOptions.create()
                        .withTimeout(config.getRequestTimeout())
                    )
                    .build()
            );
        } catch (TarantoolClientException e) {
            throw new TarantoolClientException(e);
        }
    }

    /**
     * MessagePack value mapper configured with an ArrayValue to tuple converter corresponding to the selected
     * tuple type
     *
     * @return configured mapper with {@link ArrayValue} to {@code T} converter
     */
    protected abstract CallResultMapper<R, SingleValueCallResult<R>> tupleResultMapper();

    private CompletableFuture<R> executeOperation(ProxyOperation<R> operation) {
        return operation.execute();
    }

    private CompletableFuture<Void> executeVoidOperation(ProxyOperation<Void> operation) {
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
