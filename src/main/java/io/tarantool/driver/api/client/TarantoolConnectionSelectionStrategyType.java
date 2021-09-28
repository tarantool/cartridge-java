package io.tarantool.driver.api.client;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;

/**
 * Enumeration of selection strategy type.
 * <p>
 * It used for getting factory which setting strategy connection choosing.
 *
 * @author Oleg Kuznetsov
 */
public enum TarantoolConnectionSelectionStrategyType {

    ROUND_ROBIN(RoundRobinStrategyFactory.INSTANCE),
    PARALLEL_ROUND_ROBIN(ParallelRoundRobinStrategyFactory.INSTANCE);

    private final ConnectionSelectionStrategyFactory value;

    TarantoolConnectionSelectionStrategyType(ConnectionSelectionStrategyFactory value) {
        this.value = value;
    }

    /**
     * Value of enum
     *
     * @return {@link ConnectionSelectionStrategyFactory}
     */
    public ConnectionSelectionStrategyFactory value() {
        return value;
    }

    /**
     * @return default strategy type
     */
    static TarantoolConnectionSelectionStrategyType defaultType() {
        return TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
    }
}
