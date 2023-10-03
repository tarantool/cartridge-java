package io.tarantool.driver.api;

import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

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
     * Configure the mapper with default {@link Collection} to {@code MP_ARRAY} entity converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultCollectionObjectConverter();

    /**
     * Configure the mapper with default {@link List} to {@code MP_ARRAY} entity converter
     *
     * @return builder
     */
    MessagePackMapperBuilder withDefaultListObjectConverter();

    /**
     * Configure the mapper with a specified MessagePack entity-to-object converter
     *
     * @param valueType MessagePack source type
     * @param converter MessagePack entity-to-object and object-to-entity converter
     * @param <V>       MessagePack's entity type that the converter accepts and/or returns
     * @param <O>       java object's type that the converter accepts and/or returns
     * @return builder
     * @see io.tarantool.driver.mappers.DefaultMessagePackMapper#registerValueConverter(ValueType, ValueConverter)
     */
    <V extends Value, O> MessagePackMapperBuilder
    withValueConverter(ValueType valueType, ValueConverter<V, O> converter);

    /**
     * Configure the mapper with a specified MessagePack entity-to-object converter
     *
     * @param valueType   MessagePack source type
     * @param objectClass target object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack's entity type that the converter accepts and/or returns
     * @param <O>         java object's type that the converter accepts and/or returns
     * @return builder
     * @see DefaultMessagePackMapper#registerValueConverter(ValueType, Class, ValueConverter)
     */
    <V extends Value, O> MessagePackMapperBuilder withValueConverter(
        ValueType valueType,
        Class<O> objectClass,
        ValueConverter<V, O> converter);

    /**
     * Configure the mapper with a specified MessagePack object-to-entity converter
     *
     * @param converter MessagePack entity-to-object and object-to-entity converter
     * @param <V>       MessagePack's entity type that the converter accepts and/or returns
     * @param <O>       java object's type that the converter accepts and/or returns
     * @return builder
     */
    <V extends Value, O> MessagePackMapperBuilder withObjectConverter(ObjectConverter<O, V> converter);

    /**
     * Configure the mapper with a specified MessagePack object-to-entity converter
     *
     * @param objectClass source object class
     * @param converter   MessagePack entity-to-object and object-to-entity converter
     * @param <V>         MessagePack's entity type that the converter accepts and/or returns
     * @param <O>         java object's type that the converter accepts and/or returns
     * @return builder
     */
    <V extends Value, O> MessagePackMapperBuilder withObjectConverter(
        Class<O> objectClass,
        ObjectConverter<O, V> converter);

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
    <V extends Value, O> MessagePackMapperBuilder withObjectConverter(
        Class<O> objectClass, Class<V> valueClass,
        ObjectConverter<O, V> converter);

    /**
     * Build the mapper instance
     *
     * @return a new mapper instance
     */
    MessagePackMapper build();
}
