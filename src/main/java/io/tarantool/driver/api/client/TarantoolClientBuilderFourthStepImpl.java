package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

public class TarantoolClientBuilderFourthStepImpl implements TarantoolClientBuilderFourthStep {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;

    public TarantoolClientBuilderFourthStepImpl(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    @Override
    public TarantoolClientBuilderRetryingFirstStep withRetryAttemptsInAmount(int amountOfAttempts) {
        return new TarantoolClientBuilderRetryingFirstStepImpl(this.tarantoolClient,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(5));
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return tarantoolClient;
    }
}
