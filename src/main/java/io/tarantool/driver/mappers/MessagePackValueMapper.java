package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.Optional;

/**
 * Basic interface for collection of generic converters between MessagePack entities and Java objects.
 * Value converters must be added using the {@link #registerValueConverter(ValueType, Class, ValueConverter)} method
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public interface MessagePackValueMapper {
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
     * Create Java object out of its MessagePack representation. Converters will be checked to match the target
     * object type.
     * @param v MessagePack entity
     * @param targetClass Java object class
     * @param <V> source MessagePack entity type
     * @param <O> target object type
     * @return Java object
     * @throws MessagePackValueMapperException if the corresponding conversion cannot be performed
     */
    <V extends Value, O> O fromValue(V v, Class<O> targetClass) throws MessagePackValueMapperException;

    /**
     * Adds a MessagePack entity converter to this mappers instance.
     * @param valueType  MessagePack source type
     * @param objectClass target object class
     * @param converter object-to-entity converter
     * @param <V>       MessagePack's entity type that the converter accepts and/or returns
     * @param <O>       java object's type that the converter accepts and/or returns
     * @see ValueConverter
     */
    <V extends Value, O> void registerValueConverter(ValueType valueType, Class<? extends O> objectClass,
                                                     ValueConverter<V, ? extends O> converter);

    /**
     * Get a converter capable of converting from the source entity class to the target class
     * @param valueType   MessagePack source type
     * @param objectClass the target conversion class
     * @param <V>         MessagePack's entity type that the converter accepts and/or returns
     * @param <O>         java object's type that the converter accepts and/or returns
     * @return a nullable converter instance wrapped in {@code Optional}
     */
    <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(ValueType valueType, Class<O> objectClass);
}
