package io.tarantool.driver.api;

import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.Value;

import java.util.List;
import java.util.Map;

/**
 * Builder for {@link MessagePackMapper}
 */
public interface MessagePackMapperBuilder {
    /**
     * Configure the mapper with default {@code MP_MAP} entity to {@link Map} converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultMapValueConverter();

    /**
     * Configure the mapper with default {@link Map} to {@code MP_MAP} entity converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultMapObjectConverter();

    /**
     * Configure the mapper with default {@code MP_ARRAY} entity to {@link List} converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultArrayValueConverter();

    /**
     * Configure the mapper with default {@link List} to {@code MP_ARRAY} entity converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultListObjectConverter();

    /**
     * Configure the mapper with a specified MessagePack entity-to-object and object-to-entity converter
     *
     * @param valueClass  MessagePack entity class
     * @param objectClass object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack entity type
     * @param <O>         object type
     * @param <T>         converter type
     * @return builder
     */
    <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> MessagePackMapperBuilder withConverter(
            Class<V> valueClass, Class<O> objectClass, T converter);

    /**
     * Configure the mapper with a specified MessagePack entity-to-object converter
     *
     * @param converter MessagePack entity-to-object and object-to-entity converter
     * @param <V>       MessagePack entity type
     * @param <O>       object type
     * @return builder
     * @see io.tarantool.driver.mappers.DefaultMessagePackMapper#registerValueConverter(ValueConverter)
     */
    <V extends Value, O> MessagePackMapperBuilder withValueConverter(ValueConverter<V, O> converter);

    /**
     * Configure the mapper with a specified MessagePack entity-to-object converter
     *
     * @param valueClass source entity class
     * @param converter  MessagePack entity-to-object and object-to-entity converter
     * @param <V>        MessagePack entity type
     * @param <O>        object type
     * @return builder
     * @see io.tarantool.driver.mappers.DefaultMessagePackMapper#registerValueConverter(Class, ValueConverter)
     */
    <V extends Value, O> MessagePackMapperBuilder
    withValueConverter(Class<V> valueClass, ValueConverter<V, O> converter);

    /**
     * Configure the mapper with a specified MessagePack entity-to-object converter
     *
     * @param valueClass  source entity class
     * @param objectClass target object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack entity type
     * @param <O>         object type
     * @return builder
     * @see io.tarantool.driver.mappers.DefaultMessagePackMapper#registerValueConverter(Class, Class, ValueConverter)
     */
    <V extends Value, O> MessagePackMapperBuilder withValueConverter(Class<? extends V> valueClass,
                                                                     Class<O> objectClass,
                                                                     ValueConverter<V, O> converter);

    /**
     * Configure the mapper with a specified MessagePack object-to-entity converter
     *
     * @param converter MessagePack entity-to-object and object-to-entity converter
     * @param <V>       MessagePack entity type
     * @param <O>       object type
     * @return builder
     */
    <V extends Value, O> MessagePackMapperBuilder withObjectConverter(ObjectConverter<O, V> converter);

    /**
     * Configure the mapper with a specified MessagePack object-to-entity converter
     *
     * @param objectClass source object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack entity type
     * @param <O>         object type
     * @return builder
     */
    <V extends Value, O> MessagePackMapperBuilder
    withObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter);

    /**
     * Configure the mapper with a specified MessagePack object-to-entity converter
     *
     * @param objectClass source object class
     * @param valueClass  target object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack entity type
     * @param <O>         object type
     * @return builder
     */
    <V extends Value, O> MessagePackMapperBuilder withObjectConverter(Class<O> objectClass, Class<V> valueClass,
                                                                      ObjectConverter<O, V> converter);

    /**
     * Build the mapper instance
     *
     * @return a new mapper instance
     */
    MessagePackMapper build();
}
