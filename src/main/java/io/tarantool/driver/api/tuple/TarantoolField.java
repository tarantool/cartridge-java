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

    /**
     * Get the field value, possibly converted to some Java type
     *
     * @param mapper mapper for converting MessagePack entity to Java object
     * @return value
     */
    Object getValue(MessagePackValueMapper mapper);

    /**
     * Check whether the underlying field value can be converted to an object using the given MessagePack-to-object
     * mapper
     *
     * @param targetClass the target type class
     * @param mapper mapper for converting MessagePack entity to Java object
     * @return value
     */
    boolean canConvertValue(Class<?> targetClass, MessagePackValueMapper mapper);
}
