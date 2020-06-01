package io.tarantool.driver.mappers;

import org.msgpack.value.Value;

/**
 * Basic interface for converters from Java objects to MessagePack entities for a particular class
 *
 * @author Alexey Kuzin
 */
public interface ObjectConverter<T, S extends Value> {
    /**
     * Convert Java object to a MessagePack entity
     * @param object object
     * @return entity
     */
    S toValue(T object);

    /**
     * Optional method for determining if this specific object can be converted to the specified {@link Value} type.
     * @param object the object to be converted
     * @return true, if the object csn be converted
     */
    default boolean canConvertObject(T object) {
        return true;
    }
}
