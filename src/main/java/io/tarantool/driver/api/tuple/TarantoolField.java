package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.protocol.Packable;

/**
 * Represents individual field in a tuple
 *
 * @author Alexey Kuzin
 */
public interface TarantoolField extends Packable {
    /**
     * Get the field value converted to the target type
     *
     * @param targetClass the target type class
     * @param mapper mapper for converting MessagePack entity to Java object
     * @param <O> the target type
     * @return value
     */
    <O> O getValue(Class<O> targetClass, MessagePackValueMapper mapper);
}
