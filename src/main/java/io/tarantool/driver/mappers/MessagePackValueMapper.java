package io.tarantool.driver.mappers;

import org.msgpack.value.Value;

import java.util.Optional;

/**
 * Basic interface for collection of generic converters between MessagePack entities and Java objects.
 * Value converters must be added using the {@link #registerValueConverter(Class, ValueConverter)} method
 *
 * @author Alexey Kuzin
 */
public interface MessagePackValueMapper extends Cloneable {
    /**
     * Create Java object out of its MessagePack representation.
     * @param v MessagePack entity
     * @param <V> source MessagePack entity type
     * @param <O> target object type
     * @return Java object
     * @throws MessagePackValueMapperException if the corresponding conversion cannot be performed
     */
    <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException;

    /**
     * Adds a MessagePack entity converter to this mappers instance.
     * @param valueClass entity class to register the converter for
     * @param converter object-to-entity converter
     * @param <V> source MessagePack entity type
     * @param <O> target object type
     * @see ValueConverter
     */
    <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter);

    /**
     * Get a converter capable of converting the target class
     * @param objectClass the target conversion class
     * @param <V> source MessagePack entity type
     * @param <O> target object type
     * @return a nullable converter instance wrapped in {@code Optional}
     */
    <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<O> objectClass);
}
