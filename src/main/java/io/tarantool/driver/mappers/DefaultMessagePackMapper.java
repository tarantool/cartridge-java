package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MessagePackMapperBuilder;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.converters.ConverterWrapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultCollectionToArrayValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultListToArrayValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultMapToMapValueConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultArrayValueToListConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultMapValueToMapConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultNullToNilValueConverter;
import org.msgpack.value.NilValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.tarantool.driver.mappers.MapperReflectionUtils.getInterfaceParameterClass;

/**
 * Default implementation of {@link MessagePackObjectMapper} and {@link MessagePackValueMapper}.
 * Deals with standard Java objects
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public class DefaultMessagePackMapper implements MessagePackMapper {

    private static final long serialVersionUID = 20220418L;

    private final Map<ValueType, List<ConverterWrapper<ValueConverter<? extends Value, ?>>>> valueConverters;
    private final Map<String, List<ConverterWrapper<ObjectConverter<?, ? extends Value>>>> objectConverters;
    private final ObjectConverter<Object, NilValue> nilConverter = new DefaultNullToNilValueConverter();

    /**
     * Basic constructor
     */
    public DefaultMessagePackMapper() {
        valueConverters = new HashMap<>();
        objectConverters = new HashMap<>();
    }

    /**
     * Copying constructor
     *
     * @param mapper another mapper instance
     */
    public DefaultMessagePackMapper(DefaultMessagePackMapper mapper) {
        this.valueConverters = mapper.valueConverters.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
        this.objectConverters = mapper.objectConverters.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));
    }

    @Override
    public <V extends Value, O> O fromValue(V v) {
        Optional<ValueConverter<V, O>> converter = getValueConverter(v, v.getValueType());
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ValueConverter for type %s is not found", v.getClass());
        }
        return converter.get().fromValue(v);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(V v, ValueType valueType) {
        ValueConverter<V, O> converter;
        List<ConverterWrapper<ValueConverter<? extends Value, ?>>> wrappers =
            valueConverters.getOrDefault(valueType, Collections.emptyList());
        for (int i = 0; i < wrappers.size(); i++) {
            converter = (ValueConverter<V, O>) wrappers.get(i).getConverter();
            if (converter.canConvertValue(v)) {
                return Optional.of(converter);
            }
        }
        return Optional.empty();
    }

    @Override
    public <V extends Value, O> O fromValue(V v, Class<O> targetClass) {
        Optional<ValueConverter<V, O>> converter = getValueConverter(v, v.getValueType(), targetClass);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException(
                "ValueConverter for type %s and target class %s is not found", v.getClass(), targetClass);
        }
        return converter.get().fromValue(v);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(
        V v, ValueType valueType, Class<O> targetClass) {
        ValueConverter<V, O> converter;
        List<ConverterWrapper<ValueConverter<? extends Value, ?>>> wrappers =
            valueConverters.getOrDefault(valueType, Collections.emptyList());
        ConverterWrapper<ValueConverter<? extends Value, ?>> wrapper;
        for (int i = 0; i < wrappers.size(); i++) {
            wrapper = wrappers.get(i);
            if (checkConverterByTargetType(wrapper.getTargetClass(), targetClass)) {
                converter = (ValueConverter<V, O>) wrapper.getConverter();
                if (converter.canConvertValue(v)) {
                    return Optional.of(converter);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkConverterByTargetType(Class<?> targetClassOfConverter, Class<?> targetClass) {
        return targetClassOfConverter.isAssignableFrom(targetClass);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverterByTargetType(
        ValueType valueType, Class<? extends O> targetClass) {
        List<ConverterWrapper<ValueConverter<? extends Value, ?>>> wrappers =
            valueConverters.getOrDefault(valueType, Collections.emptyList());
        ConverterWrapper<ValueConverter<? extends Value, ?>> wrapper;
        for (int i = 0; i < wrappers.size(); i++) {
            wrapper = wrappers.get(i);
            if (checkConverterByTargetType(wrapper.getTargetClass(), targetClass)) {
                return Optional.of((ValueConverter<V, O>) wrapper.getConverter());
            }
        }
        return Optional.empty();
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(
        ValueType valueType, Class<? extends O> targetClass) {
        return getValueConverterByTargetType(valueType, targetClass);
    }

    /**
     * Perform {@link ValueConverter} converter registration. The target object class for registration is determined
     * automatically
     *
     * @param valueType MessagePack source type
     * @param converter entity-to-object converter
     * @param <V>       MessagePack's entity type that the converter accepts and/or returns
     * @param <O>       java object's type that the converter accepts and/or returns
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(
        ValueType valueType,
        ValueConverter<V, ? extends O> converter) {
        try {
            Class<O> objectClass = getInterfaceParameterClass(converter, ValueConverter.class, 1);
            registerValueConverter(valueType, objectClass, converter);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            throw new TarantoolClientException("Failed to determine the target parameter type of the generic " +
                "interface, try to use the method registerValueConverter(valueClass, objectClass, converter) " +
                "for registering the converter");
        }
    }

    @Override
    public <V extends Value, O> void registerValueConverter(
        ValueType valueType,
        Class<? extends O> objectClass,
        ValueConverter<V, ? extends O> converter) {
        List<ConverterWrapper<ValueConverter<? extends Value, ?>>> converters =
            valueConverters.computeIfAbsent(valueType, k -> new ArrayList<>());
        converters.add(0, new ConverterWrapper<>(converter, objectClass));
    }

    public <V extends Value, O> void registerValueConverterWithoutTargetClass(
        ValueType valueType,
        ValueConverter<V, ? extends O> converter) {
        List<ConverterWrapper<ValueConverter<? extends Value, ?>>> converters =
            valueConverters.computeIfAbsent(valueType, k -> new ArrayList<>());
        converters.add(0, new ConverterWrapper<>(converter, Object.class));
    }

    @Override
    public <V extends Value, O> V toValue(O o) {
        ObjectConverter<O, V> converter = getObjectConverter(o);
        return converter.toValue(o);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> ObjectConverter<O, V> getObjectConverter(O o) {
        if (o == null) {
            return (ObjectConverter<O, V>) nilConverter;
        }
        Optional<ObjectConverter<O, V>> converter = findObjectConverter(o, o.getClass());
        if (!converter.isPresent()) {
            throw new MessagePackObjectMapperException("ObjectConverter for type %s is not found", o.getClass());
        }
        return converter.get();
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverter(O o, String typeName) {
        ObjectConverter<O, V> converter;
        List<ConverterWrapper<ObjectConverter<?, ? extends Value>>> wrappers =
            objectConverters.getOrDefault(typeName, Collections.emptyList());
        for (int i = 0; i < wrappers.size(); i++) {
            converter = (ObjectConverter<O, V>) wrappers.get(i).getConverter();
            if (converter.canConvertObject(o)) {
                return Optional.of(converter);
            }
        }
        return Optional.empty();
    }

    private <V extends Value, O> Optional<ObjectConverter<O, V>> findObjectConverter(O o, Class<?> objectClass) {
        Optional<ObjectConverter<O, V>> converter = getObjectConverter(o, objectClass.getTypeName());
        if (!converter.isPresent() && objectClass.getSuperclass() != null) {
            converter = findObjectConverter(o, objectClass.getSuperclass());
        }
        if (!converter.isPresent()) {
            for (Class<?> iface : objectClass.getInterfaces()) {
                converter = findObjectConverter(o, iface);
                if (converter.isPresent()) {
                    break;
                }
            }
        }
        return converter;
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverterByTargetType(
        String typeName, Class<? extends V> valueClass) {
        List<ConverterWrapper<ObjectConverter<?, ? extends Value>>> wrappers =
            objectConverters.getOrDefault(typeName, Collections.emptyList());
        ConverterWrapper<ObjectConverter<?, ? extends Value>> wrapper;
        for (int i = 0; i < wrappers.size(); i++) {
            wrapper = wrappers.get(i);
            if (checkConverterByTargetType(wrapper.getTargetClass(), valueClass)) {
                return Optional.of((ObjectConverter<O, V>) wrapper.getConverter());
            }
        }
        return Optional.empty();
    }

    private <V extends Value, O> Optional<ObjectConverter<O, V>> findObjectConverter(
        Class<?> objectClass, Class<? extends V> valueClass) {
        Optional<ObjectConverter<O, V>> converter = getObjectConverterByTargetType(
            objectClass.getTypeName(), valueClass);
        if (!converter.isPresent() && objectClass.getSuperclass() != null) {
            converter = findObjectConverter(objectClass.getSuperclass(), valueClass);
        }
        if (!converter.isPresent()) {
            for (Class<?> iface : objectClass.getInterfaces()) {
                converter = findObjectConverter(iface, valueClass);
                if (converter.isPresent()) {
                    break;
                }
            }
        }
        return converter;
    }

    @Override
    public <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverter(
        Class<? extends O> objectClass, Class<? extends V> valueClass) {
        return findObjectConverter(objectClass, valueClass);
    }

    /**
     * Perform {@link ObjectConverter} converter registration. The source object class and target entity class for
     * registration are determined automatically
     *
     * @param converter object-to-entity converter
     * @param <V>       MessagePack's entity type that the converter accepts and/or returns
     * @param <O>       java object's type that the converter accepts and/or returns
     * @see ObjectConverter
     */
    public <V extends Value, O> void registerObjectConverter(ObjectConverter<O, V> converter) {
        try {
            registerObjectConverter(
                getInterfaceParameterClass(converter, ObjectConverter.class, 0), converter);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            throw new TarantoolClientException("Failed to determine the target parameter type of the generic " +
                "interface, try to use the method registerObjectConverter(objectClass, valueClass, converter) " +
                "for registering the converter");
        }
    }

    /**
     * Adds a Java object converter to this mappers instance. The target value class for registration is determined
     * automatically
     *
     * @param objectClass object class to register the converter for
     * @param converter   entity-to-object converter
     * @param <V>         the target MessagePack entity type
     * @param <O>         the source object type
     * @see ObjectConverter
     */
    public <V extends Value, O> void registerObjectConverter(
        Class<? extends O> objectClass, ObjectConverter<O, V> converter) {
        try {
            Class<V> valueClass = getInterfaceParameterClass(converter, ObjectConverter.class, 1);
            registerObjectConverter(objectClass, valueClass, converter);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            throw new TarantoolClientException("Failed to determine the target parameter type of the generic " +
                "interface, try to use the method registerObjectConverter(objectClass, valueClass, converter) " +
                "for registering the converter");
        }
    }

    @Override
    public <V extends Value, O> void registerObjectConverter(
        Class<? extends O> objectClass,
        Class<V> valueClass,
        ObjectConverter<O, V> converter) {
        List<ConverterWrapper<ObjectConverter<?, ? extends Value>>> converters =
            objectConverters.computeIfAbsent(objectClass.getTypeName(), k -> new ArrayList<>());
        converters.add(0, new ConverterWrapper<>(converter, valueClass));
    }

    @Override
    public MessagePackMapper copy() {
        return new DefaultMessagePackMapper(this);
    }

    /**
     * Builder for {@link DefaultMessagePackMapper}
     */
    public static class Builder implements MessagePackMapperBuilder {
        private final DefaultMessagePackMapper mapper;

        /**
         * Basic constructor, initialized with an empty mapper
         */
        public Builder() {
            mapper = new DefaultMessagePackMapper();
        }

        /**
         * Basic constructor, initialized with the specified mapper
         *
         * @param mapper a mapper instance
         */
        public Builder(DefaultMessagePackMapper mapper) {
            this.mapper = new DefaultMessagePackMapper(mapper);
        }

        @Override
        public Builder withDefaultMapValueConverter() {
            mapper.registerValueConverter(ValueType.MAP, new DefaultMapValueToMapConverter(mapper));
            return this;
        }

        @Override
        public Builder withDefaultMapObjectConverter() {
            DefaultMapToMapValueConverter converter = new DefaultMapToMapValueConverter(mapper);
            mapper.registerObjectConverter(converter);
            Class<HashMap<?, ?>> cls = (Class<HashMap<?, ?>>) (Object) HashMap.class;
            mapper.registerObjectConverter(cls, converter);
            return this;
        }

        @Override
        public Builder withDefaultArrayValueConverter() {
            mapper.registerValueConverter(ValueType.ARRAY, new DefaultArrayValueToListConverter(mapper));
            return this;
        }

        @Override
        public Builder withDefaultCollectionObjectConverter() {
            DefaultCollectionToArrayValueConverter converter = new DefaultCollectionToArrayValueConverter(mapper);
            mapper.registerObjectConverter(converter);
            mapper.registerObjectConverter((Class<Collection<?>>) (Object) Collection.class, converter);
            mapper.registerObjectConverter((Class<AbstractCollection<?>>) (Object) AbstractCollection.class, converter);
            return this;
        }

        @Override
        public Builder withDefaultListObjectConverter() {
            DefaultListToArrayValueConverter converter = new DefaultListToArrayValueConverter(mapper);
            mapper.registerObjectConverter(converter);
            Class<ArrayList<?>> cls = (Class<ArrayList<?>>) (Object) ArrayList.class;
            mapper.registerObjectConverter(cls, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withValueConverter(ValueType valueType, ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueType, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withValueConverter(
            ValueType valueType, Class<O> objectClass,
            ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueType, objectClass, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withObjectConverter(ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(objectClass, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withObjectConverter(
            Class<O> objectClass, Class<V> valueClass,
            ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(objectClass, valueClass, converter);
            return this;
        }

        @Override
        public DefaultMessagePackMapper build() {
            return mapper;
        }
    }
}
