package io.tarantool.driver;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.space.RetryingTarantoolSpaceOperations;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;

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
    protected
    RetryingTarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>>
    spaceOperations(TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> decoratedSpaceOperations,
                    RequestRetryPolicyFactory retryPolicyFactory, Executor executor) {
        return new RetryingTarantoolSpaceOperations<>(decoratedSpaceOperations, retryPolicyFactory, executor);
    }
}
