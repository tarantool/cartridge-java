package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder;

import java.util.function.Function;

public class TarantoolClientBuilderRetryingFirstStepImpl implements TarantoolClientBuilderRetryingFirstStep {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private final Builder<Function<Throwable, Boolean>> retryPolicyBuilder;

    public TarantoolClientBuilderRetryingFirstStepImpl(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient, Builder<Function<Throwable, Boolean>> builder) {
        this.tarantoolClient = tarantoolClient;
        this.retryPolicyBuilder = builder;
    }

    @Override
    public TarantoolClientBuilderRetryingSecondStep withDelay(long delayMs) {
        this.retryPolicyBuilder.withDelay(delayMs);
        return new TarantoolClientBuilderRetryingSecondStepImpl(tarantoolClient, retryPolicyBuilder);
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return new RetryingTarantoolTupleClient(tarantoolClient, retryPolicyBuilder.build());
    }
}
