package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.protocol.Packable;

import java.util.Optional;

/**
 * Basic Tarantool atom of data
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTuple extends Iterable<TarantoolField>, Packable {
    /**
     * Get a tuple field by its position
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @return field or empty optional if the field position is out of tuple length
     */
    Optional<TarantoolField> getField(int fieldPosition);

    /**
     * Get a tuple field value by its position specifying the target value type
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param objectClass the target value type class
     * @param <O> the target value type
     * @return nullable value of a field wrapped in optional
     * @throws TarantoolValueConverterNotFoundException if the converter for the target type is not found
     */
    <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass) throws TarantoolValueConverterNotFoundException;
}
