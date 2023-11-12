package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.ObjectConverter;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Basic interface for generic converters between Java objects and MessagePack entities.
 * Object converters must be added using the {@link #registerObjectConverter(Class, Class, ObjectConverter)} method
 *
 * @author Alexey Kuzin
 */
public interface MessagePackObjectMapper {
    /**
     * Create MessagePack entity representation for an object.
     *
     * @param o   an object to be converted
     * @param <V> the target MessagePack entity type
     * @param <O> the source object type
     * @return instance of MessagePack {@link Value}
     * @throws MessagePackObjectMapperException if the corresponding conversion cannot be performed
     */
    <V extends Value, O> V toValue(O o) throws MessagePackObjectMapperException;

    /**
     * Adds a Java object converter to this mappers instance
     *
     * @param objectClass source object class
     * @param valueClass  target value class
     * @param converter   entity-to-object converter
     * @param <V>         the target MessagePack entity type
     * @param <O>         the source object type
     * @see ObjectConverter
     */
    <V extends Value, O> void registerObjectConverter(
        Class<? extends O> objectClass, Class<V> valueClass,
        ObjectConverter<O, V> converter);

    <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverter(
        Class<? extends O> objectClass, Class<? extends V> valueClass);
}
