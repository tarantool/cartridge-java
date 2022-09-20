package io.tarantool.driver.core.space;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.retry.RequestRetryPolicy;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.DeleteOptions;
import io.tarantool.driver.api.space.options.InsertManyOptions;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.ReplaceManyOptions;
import io.tarantool.driver.api.space.options.ReplaceOptions;
import io.tarantool.driver.api.space.options.SelectOptions;
import io.tarantool.driver.api.space.options.UpdateOptions;
import io.tarantool.driver.api.space.options.UpsertOptions;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Wrapper for {@link TarantoolSpaceOperations} instances which adds request retry policy to each operation
 *
 * @param <T> target tuple type
 * @param <R> target tuple collection type
 * @author Alexey Kuzin
 */
public class RetryingTarantoolSpace<T extends Packable, R extends Collection<T>>
        implements TarantoolSpaceOperations<T, R> {

    private final TarantoolSpaceOperations<T, R> spaceOperations;
    private final RequestRetryPolicyFactory retryPolicyFactory;
    private final Executor executor;

    /**
     * Basic constructor
     *
     * @param spaceOperations    {@link TarantoolSpaceOperations} instance which operations will be wrapped
     * @param retryPolicyFactory request retrying policy factory
     * @param executor           executor service for retry callbacks
     */
    public RetryingTarantoolSpace(TarantoolSpaceOperations<T, R> spaceOperations,
                                  RequestRetryPolicyFactory retryPolicyFactory,
                                  Executor executor) {
        this.spaceOperations = spaceOperations;
        this.retryPolicyFactory = retryPolicyFactory;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<R> delete(Conditions conditions)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.delete(conditions));
    }

    @Override
    public CompletableFuture<R> delete(Conditions conditions, DeleteOptions options)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.delete(conditions, options));
    }

    @Override
    public CompletableFuture<R> insert(T tuple) throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.insert(tuple));
    }

    @Override
    public CompletableFuture<R> insert(T tuple, InsertOptions options) throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.insert(tuple, options));
    }

    @Override
    public CompletableFuture<R> insertMany(Collection<T> tuples) throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.insertMany(tuples));
    }

    @Override
    public CompletableFuture<R> insertMany(Collection<T> tuples, InsertManyOptions options)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.insertMany(tuples, options));
    }

    @Override
    public CompletableFuture<R> replace(T tuple)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.replace(tuple));
    }

    @Override
    public CompletableFuture<R> replace(T tuple, ReplaceOptions options)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.replace(tuple, options));
    }

    @Override
    public CompletableFuture<R> replaceMany(Collection<T> tuples)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.replaceMany(tuples));
    }

    @Override
    public CompletableFuture<R> replaceMany(Collection<T> tuples, ReplaceManyOptions options)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.replaceMany(tuples, options));
    }

    @Override
    public CompletableFuture<R> select(Conditions conditions)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.select(conditions));
    }

    @Override
    public CompletableFuture<R> select(Conditions conditions, SelectOptions options)
            throws TarantoolClientException {
        return wrapOperation(() -> spaceOperations.select(conditions, options));
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, T tuple) {
        return wrapOperation(() -> spaceOperations.update(conditions, tuple));
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, T tuple, UpdateOptions options) {
        return wrapOperation(() -> spaceOperations.update(conditions, tuple, options));
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, TupleOperations operations) {
        return wrapOperation(() -> spaceOperations.update(conditions, operations));
    }

    @Override
    public CompletableFuture<R> update(Conditions conditions, TupleOperations operations, UpdateOptions options) {
        return wrapOperation(() -> spaceOperations.update(conditions, operations, options));
    }

    @Override
    public CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations) {
        return wrapOperation(() -> spaceOperations.upsert(conditions, tuple, operations));
    }

    @Override
    public CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations,
                                       UpsertOptions options) {
        return wrapOperation(() -> spaceOperations.upsert(conditions, tuple, operations, options));
    }

    @Override
    public CompletableFuture<Void> truncate() throws TarantoolClientException {
        return wrapVoidOperation(spaceOperations::truncate);
    }

    @Override
    public TarantoolSpaceMetadata getMetadata() {
        return spaceOperations.getMetadata();
    }

    @Override
    public TarantoolCursor<T> cursor(Conditions conditions, int batchSize) {
        return spaceOperations.cursor(conditions, batchSize);
    }

    @Override
    public TarantoolCursor<T> cursor(Conditions conditions) {
        return spaceOperations.cursor(conditions);
    }

    private CompletableFuture<R> wrapOperation(Supplier<CompletableFuture<R>> operation) {
        RequestRetryPolicy retryPolicy = retryPolicyFactory.create();
        return retryPolicy.wrapOperation(operation, executor);
    }

    private CompletableFuture<Void> wrapVoidOperation(Supplier<CompletableFuture<Void>> operation) {
        RequestRetryPolicy retryPolicy = retryPolicyFactory.create();
        return retryPolicy.wrapOperation(operation, executor);
    }
}
