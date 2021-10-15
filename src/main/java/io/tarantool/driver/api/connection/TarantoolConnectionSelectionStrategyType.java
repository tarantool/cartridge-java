package io.tarantool.driver.api.connection;

import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;

/**
 * Enumeration of the default types of connection selection strategies.
 * <p>
 * Provides shortcuts for the built-in connection selection strategy factory types.
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
