package io.tarantool.driver.mappers;

import org.msgpack.value.Value;

/**
 * Basic interface for converters between Java objects and MessagePack entities
 *
 * @author Alexey Kuzin
 */
public interface MessagePackObjectMapper {
    /**
     * Create MessagePack entity representation for an object.
     * @param o an object to be converted
     * @throws MessagePackValueMapperException if the corresponding conversion cannot be performed
     * @return instance of MessagePack {@link Value}
     */
    <V extends Value, O> V toValue(O o) throws MessagePackValueMapperException;

    /**
     * Create Java object out of its MessagePack representation.
     * @param v MessagePack entity
     * @throws MessagePackValueMapperException if the corresponding conversion cannot be performed
     * @return Java object
     */
    <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException;

    /**
     * Adds a MessagePack entity converter to this mappers instance.
     * @param valueClass entity class to register the converter for
     * @param converter object-to-entity converter
     * @see ValueConverter
     */
    <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter);

    /**
     * Adds a Java object converter to this mappers instance
     * @param objectClass object class to register the converter for
     * @param converter entity-to-object converter
     * @see ObjectConverter
     */
    <V extends Value, O> void registerObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter);
}
