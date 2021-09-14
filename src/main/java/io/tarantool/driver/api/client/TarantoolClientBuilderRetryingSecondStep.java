package io.tarantool.driver.api.client;

public interface TarantoolClientBuilderRetryingSecondStep extends TarantoolClientBuilderCompletable {

    TarantoolClientBuilderRetryingSecondStep withRequestTimeout(long requestTimeout);
}
