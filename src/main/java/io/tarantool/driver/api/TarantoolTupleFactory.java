package io.tarantool.driver.api;

import io.tarantool.driver.api.tuple.TarantoolTuple;

import java.util.Collection;

/**
 * A factory for {@link TarantoolTuple} instances
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTupleFactory {
    /**
     * Create an empty tuple
     * @return new tuple instance
     */
    TarantoolTuple create();

    /**
     * Create a tuple from an array of field values
     * @param fields array of field values, may contain null values
     * @return new tuple instance
     */
    TarantoolTuple create(Object... fields);

    /**
     * Create a tuple from a collection of field values
     * @param fields collection of field values, may contain null values
     * @return new tuple instance
     */
    TarantoolTuple create(Collection<?> fields);
}
