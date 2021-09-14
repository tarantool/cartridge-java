package io.tarantool.driver.api.client;

public interface TarantoolClientBuilderFourthStep extends TarantoolClientBuilderCompletable {

    TarantoolClientBuilderRetryingFirstStep withRetryAttemptsInAmount(int amountOfAttempts);
}
