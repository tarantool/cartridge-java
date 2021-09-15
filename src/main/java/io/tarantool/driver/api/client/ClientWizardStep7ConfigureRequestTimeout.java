package io.tarantool.driver.api.client;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory.Builder;

import java.util.function.Function;

public class ClientWizardStep7ConfigureRequestTimeout {

    private final TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> tarantoolClient;
    private final Builder<Function<Throwable, Boolean>> retryPolicyBuilder;

    public ClientWizardStep7ConfigureRequestTimeout(TarantoolClient<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> tarantoolClient,
                                                    Builder<Function<Throwable, Boolean>> retryPolicyBuilder) {
        this.tarantoolClient = tarantoolClient;
        this.retryPolicyBuilder = retryPolicyBuilder;
    }

    public ClientWizardStep7ConfigureRequestTimeout withRequestTimeout(long requestTimeout) {
        this.retryPolicyBuilder.withRequestTimeout(requestTimeout);
        return this;
    }

    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        return new RetryingTarantoolTupleClient(tarantoolClient, retryPolicyBuilder.build());
    }
}
