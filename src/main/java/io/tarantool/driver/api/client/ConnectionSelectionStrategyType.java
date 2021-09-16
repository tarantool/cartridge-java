package io.tarantool.driver.api.client;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;

public enum ConnectionSelectionStrategyType {

    ROUND_ROBIN(RoundRobinStrategyFactory.INSTANCE),
    PARALLEL_ROUND_ROBIN(ParallelRoundRobinStrategyFactory.INSTANCE);

    private final ConnectionSelectionStrategyFactory value;

    ConnectionSelectionStrategyType(ConnectionSelectionStrategyFactory value) {
        this.value = value;
    }

    public ConnectionSelectionStrategyFactory value() {
        return value;
    }

    static ConnectionSelectionStrategyType defaultType() {
        return ConnectionSelectionStrategyType.ROUND_ROBIN;
    }
}