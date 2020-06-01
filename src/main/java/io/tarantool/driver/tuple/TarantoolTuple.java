package io.tarantool.driver.tuple;

import io.tarantool.driver.protocol.Packable;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Basic Tarantool atom of data
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTuple extends Iterable<TarantoolField<?, ? extends Value>>, Packable<ArrayValue> {
    /**
     * Get a tuple field by its position
     * @param fieldPosition
     * @return field or empty optional if the field position is out of tuple length
     */
    Optional<TarantoolField<?, ? extends Value>> get(int fieldPosition);
}
