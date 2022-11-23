package io.tarantool.driver.mappers.converters;

import org.msgpack.value.Value;

/**
 * Basic interface for converters from Java objects to MessagePack entities for a particular class
 *
 * @param <O> the source object type
 * @param <V> the target MessagePack entity type
 * @author Alexey Kuzin
 */
public interface ObjectConverter<O, V extends Value> extends Converter {
    /**
     * Convert Java object to a MessagePack entity
     *
     * @param object object
     * @return entity
     */
    V toValue(O object);

    /**
     * Optional method for determining if this specific object can be converted to the specified {@link Value} type.
     *
     * @param object the object to be converted
     * @return true, if the object csn be converted
     */
    default boolean canConvertObject(O object) {
        return true;
    }
}
