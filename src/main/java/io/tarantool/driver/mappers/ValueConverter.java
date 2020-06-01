package io.tarantool.driver.mappers;

import org.msgpack.value.Value;

/**
 * Basic interface for converters from MessagePack entities to Java objects for a particular class
 *
 * @author Alexey Kuzin
 */
public interface ValueConverter<T extends Value, S> {
    /**
     * Convert MessagePack entity to a Java object
     * @param value entity
     * @return object
     */
    S fromValue(T value);

    /**
     * Optional method for determining if this specific entity can be converted to the specified object type.
     * @param value MessagePack entity to be converted
     * @return true, if the entity csn be converted
     */
    default boolean canConvertValue(T value) {
        return true;
    }
}
