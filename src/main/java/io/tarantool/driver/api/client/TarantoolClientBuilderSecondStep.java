package io.tarantool.driver.api.client;

public interface TarantoolClientBuilderSecondStep {

    TarantoolClientBuilderThirdStep withConnectionSelectionStrategy(ConnectionSelectionStrategyType type);

    TarantoolClientBuilderThirdStep withDefaultConnectionSelectionStrategy();
}
