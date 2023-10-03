package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.space.RetryingTarantoolSpace;

import java.util.concurrent.Executor;

/**
 * {@link RetryingTarantoolClient} implementation for working with default tuples
 *
 * @author Alexey Kuzin
 */
public class RetryingTarantoolTupleClient
    extends RetryingTarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> {
    /**
     * Basic constructor
     *
     * @param decoratedClient    configured Tarantool client
     * @param retryPolicyFactory request retrying policy settings
     */
    public RetryingTarantoolTupleClient(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedClient,
        RequestRetryPolicyFactory retryPolicyFactory) {
        super(decoratedClient, retryPolicyFactory);
    }

    /**
     * Basic constructor
     *
     * @param decoratedClient    configured Tarantool client
     * @param retryPolicyFactory request retrying policy settings
     * @param executor           executor service for retry callbacks
     */
    public RetryingTarantoolTupleClient(
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedClient,
        RequestRetryPolicyFactory retryPolicyFactory,
        Executor executor) {
        super(decoratedClient, retryPolicyFactory, executor);
    }

    @Override
    protected RetryingTarantoolSpace<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    spaceOperations(
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedSpaceOperations,
        RequestRetryPolicyFactory retryPolicyFactory, Executor executor) {
        return new RetryingTarantoolSpace<>(decoratedSpaceOperations, retryPolicyFactory, executor);
    }
}
