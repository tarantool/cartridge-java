package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MessagePackMapperBuilder;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.converters.ConverterWrapper;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultListToArrayValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultMapToMapValueConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultArrayValueToListConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultMapValueToMapConverter;
import io.tarantool.driver.mappers.converters.value.defaults.DefaultNullToNilValueConverter;
import org.msgpack.value.NilValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
        this();
        this.valueConverters.putAll(mapper.valueConverters);
        this.objectConverters.putAll(mapper.objectConverters);
    }

    @SuppressWarnings("unchecked")
    private <V extends Value, O> ObjectConverter<O, V>
    getObjectConverter(O o, Function<String, Optional<ObjectConverter<O, V>>> getter) {
        if (o == null) {
            return (ObjectConverter<O, V>) nilConverter;
        }
        Optional<ObjectConverter<O, V>> converter = findObjectConverter(o.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackObjectMapperException("ObjectConverter for type %s is not found", o.getClass());
        }
        return converter.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> V toValue(O o) {
        Function<String, Optional<ObjectConverter<O, V>>> getter =
            typeName -> objectConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                .map(c -> (ObjectConverter<O, V>) c.getConverter())
                .filter(c -> c.canConvertObject(o))
                .findFirst();

        ObjectConverter<O, V> converter = getObjectConverter(o, getter);
        return converter.toValue(o);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v) {
        Function<ValueType, Optional<ValueConverter<V, O>>> getter =
            typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                .map(c -> (ValueConverter<V, O>) c.getConverter())
                .filter(c -> c.canConvertValue(v))
                .findFirst();
        return fromValue(v, getter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v, Class<O> targetClass) {
        Function<ValueType, Optional<ValueConverter<V, O>>> getter =
            typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                .filter(c -> checkConverterByTargetType(c.getTargetClass(), targetClass))
                .map(c -> (ValueConverter<V, O>) c.getConverter())
                .filter(c -> c.canConvertValue(v))
                .findFirst();
        return fromValue(v, getter);
    }

    private <V extends Value, O> O fromValue(V v, Function<ValueType, Optional<ValueConverter<V, O>>> getter) {
        Optional<ValueConverter<V, O>> converter = findValueConverter(v.getValueType(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ValueConverter for type %s is not found", v.getClass());
        }
        return converter.get().fromValue(v);
    }

    private <T> Optional<T> findObjectConverter(Class<?> objectClass, Function<String, Optional<T>> getter) {
        Optional<T> converter = getter.apply(objectClass.getTypeName());
        if (!converter.isPresent() && objectClass.getSuperclass() != null) {
            converter = findObjectConverter(objectClass.getSuperclass(), getter);
        }
        if (!converter.isPresent()) {
            for (Class<?> iface : objectClass.getInterfaces()) {
                converter = findObjectConverter(iface, getter);
                if (converter.isPresent()) {
                    break;
                }
            }
        }
        return converter;
    }

    private <T> Optional<T> findValueConverter(ValueType valueType, Function<ValueType, Optional<T>> getter) {
        return getter.apply(valueType);
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
            valueConverters.computeIfAbsent(valueType, k -> new LinkedList<>());
        converters.add(0, new ConverterWrapper<>(converter, objectClass));
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkConverterByTargetType(Class<?> targetClassOfConverter, Class<?> targetClass) {
        return targetClassOfConverter.isAssignableFrom(targetClass);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(
        ValueType valueType,
        Class<O> targetClass) {
        Function<ValueType, Optional<ValueConverter<V, O>>> getter =
            typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                .filter(c -> checkConverterByTargetType(c.getTargetClass(), targetClass))
                .map(c -> (ValueConverter<V, O>) c.getConverter())
                .findFirst();
        return findValueConverter(valueType, getter);
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
            objectConverters.computeIfAbsent(objectClass.getTypeName(), k -> new LinkedList<>());
        converters.add(0, new ConverterWrapper<>(converter, valueClass));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverter(
        Class<O> objectClass,
        Class<V> valueClass) {
        Function<String, Optional<ObjectConverter<O, V>>> getter =
            typeName -> objectConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                .filter(c -> checkConverterByTargetType(c.getTargetClass(), valueClass))
                .map(c -> (ObjectConverter<O, V>) c.getConverter())
                .findFirst();
        return findObjectConverter(objectClass, getter);
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
            mapper.registerObjectConverter(new DefaultMapToMapValueConverter(mapper));
            Class<HashMap<?, ?>> cls = (Class<HashMap<?, ?>>) (Object) HashMap.class;
            mapper.registerObjectConverter(cls, new DefaultMapToMapValueConverter(mapper));
            return this;
        }

        @Override
        public Builder withDefaultArrayValueConverter() {
            mapper.registerValueConverter(ValueType.ARRAY, new DefaultArrayValueToListConverter(mapper));
            return this;
        }

        @Override
        public Builder withDefaultListObjectConverter() {
            mapper.registerObjectConverter(new DefaultListToArrayValueConverter(mapper));
            Class<ArrayList<?>> cls = (Class<ArrayList<?>>) (Object) ArrayList.class;
            mapper.registerObjectConverter(cls, new DefaultListToArrayValueConverter(mapper));
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
