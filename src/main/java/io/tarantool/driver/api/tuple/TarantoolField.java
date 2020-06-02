package io.tarantool.driver.api.tuple;

import io.tarantool.driver.protocol.Packable;
import org.msgpack.value.Value;

/**
 * Represents individual field in a tuple
 * @param <T> field object representation type
 *
 * @author Alexey Kuzin
 */
public interface TarantoolField<T, S extends Value> extends Packable<S> {
    /**
     * Get the exact field value
     * @return value
     */
    T getValue();
}
