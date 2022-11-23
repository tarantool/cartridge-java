package io.tarantool.driver.mappers.converters;

import org.msgpack.value.Value;

/**
 * Basic interface for converters from MessagePack entities to Java objects for a particular class
 *
 * @param <V> the source MessagePack entity type
 * @param <O> the target object type
 * @author Alexey Kuzin
 */
public interface ValueConverter<V extends Value, O> extends Converter {
    /**
     * Convert MessagePack entity to a Java object
     *
     * @param value entity
     * @return object
     */
    O fromValue(V value);

    /**
     * Optional method for determining if this specific entity can be converted to the specified object type.
     *
     * @param value MessagePack entity to be converted
     * @return true, if the entity csn be converted
     */
    default boolean canConvertValue(V value) {
        return true;
    }
}
