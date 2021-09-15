package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;

public class ClientWizardStep5ConfigureRetryPolicy {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;

    public ClientWizardStep5ConfigureRetryPolicy(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient) {
        this.tarantoolClient = tarantoolClient;
    }

    public ClientWizardStep6ConfigureRetryDelay withRetryAttemptsInAmount(int amountOfAttempts) {
        return new ClientWizardStep6ConfigureRetryDelay(this.tarantoolClient,
                TarantoolRequestRetryPolicies.byNumberOfAttempts(5));
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return tarantoolClient;
    }
}
