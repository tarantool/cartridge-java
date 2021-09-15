package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder;

import java.util.function.Function;

public class ClientWizardStep6ConfigureRetryDelay {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private final Builder<Function<Throwable, Boolean>> retryPolicyBuilder;

    public ClientWizardStep6ConfigureRetryDelay(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient, Builder<Function<Throwable, Boolean>> builder) {
        this.tarantoolClient = tarantoolClient;
        this.retryPolicyBuilder = builder;
    }

    public ClientWizardStep7ConfigureRequestTimeout withDelay(long delayMs) {
        this.retryPolicyBuilder.withDelay(delayMs);
        return new ClientWizardStep7ConfigureRequestTimeout(tarantoolClient, retryPolicyBuilder);
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return new RetryingTarantoolTupleClient(tarantoolClient, retryPolicyBuilder.build());
    }
}
