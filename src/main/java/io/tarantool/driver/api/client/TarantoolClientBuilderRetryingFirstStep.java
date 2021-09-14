package io.tarantool.driver.api.client;

public interface TarantoolClientBuilderRetryingFirstStep extends TarantoolClientBuilderCompletable {

    TarantoolClientBuilderRetryingSecondStep withDelay(long delayMs);
}
