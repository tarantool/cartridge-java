package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MessagePackMapperBuilder;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.converters.value.DefaultNullToNilValueConverter;
import io.tarantool.driver.mappers.converters.ObjectConverter;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultListToArrayValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultMapToMapValueConverter;
import io.tarantool.driver.mappers.converters.value.DefaultArrayValueToListConverter;
import io.tarantool.driver.mappers.converters.value.DefaultMapValueToMapConverter;
import org.msgpack.value.NilValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableArrayValueImpl;
import org.msgpack.value.impl.ImmutableMapValueImpl;

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

    private final Map<String, List<ValueConverter<? extends Value, ?>>> valueConverters;
    private final Map<String, List<ObjectConverter<?, ? extends Value>>> objectConverters;
    private final Map<String, ValueConverter<? extends Value, ?>> valueConvertersByTarget;
    private final Map<String, ObjectConverter<?, ? extends Value>> objectConvertersByTarget;
    private final ObjectConverter<Object, NilValue> nilConverter = new DefaultNullToNilValueConverter();

    /**
     * Basic constructor
     */
    public DefaultMessagePackMapper() {
        valueConverters = new HashMap<>();
        valueConvertersByTarget = new HashMap<>();
        objectConverters = new HashMap<>();
        objectConvertersByTarget = new HashMap<>();
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
        this.valueConvertersByTarget.putAll(mapper.valueConvertersByTarget);
        this.objectConvertersByTarget.putAll(mapper.objectConvertersByTarget);
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
                        .map(c -> (ObjectConverter<O, V>) c)
                        .filter(c -> c.canConvertObject(o))
                        .findFirst();

        ObjectConverter<O, V> converter = getObjectConverter(o, getter);
        return converter.toValue(o);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v) {
        Function<String, Optional<ValueConverter<V, O>>> getter =
                typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .map(c -> (ValueConverter<V, O>) c)
                        .filter(c -> c.canConvertValue(v))
                        .findFirst();
        return fromValue(v, getter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v, Class<O> targetClass) {
        Function<String, Optional<ValueConverter<V, O>>> getter =
                typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .map(c -> (ValueConverter<V, O>) c)
                        .filter(c -> c.canConvertValue(v))
                        .filter(c -> checkConverterByTargetType(c, targetClass))
                        .findFirst();
        return fromValue(v, getter);
    }

    private <V extends Value, O> O fromValue(V v, Function<String, Optional<ValueConverter<V, O>>> getter) {
        Optional<ValueConverter<V, O>> converter = findValueConverter(v.getClass(), getter);
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

    private <T> Optional<T> findValueConverter(Class<?> objectClass, Function<String, Optional<T>> getter) {
        return getter.apply(objectClass.getTypeName());
    }

    /**
     * Perform {@link ValueConverter} converter registration. The source entity class and target object class for
     * registration are determined automatically
     *
     * @param converter entity-to-object converter
     * @param <V>       MessagePack entity type
     * @param <O>       object type
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(ValueConverter<V, ? extends O> converter) {
        try {
            registerValueConverter(
                    getInterfaceParameterClass(converter, ValueConverter.class, 0), converter);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            throw new TarantoolClientException("Failed to determine the source parameter type of the generic " +
                    "interface, try to use the method registerValueConverter(valueClass, objectClass, converter) " +
                    "for registering the converter");
        }
    }

    /**
     * Perform {@link ValueConverter} converter registration. The target object class for registration is determined
     * automatically
     *
     * @param valueClass source entity class
     * @param converter  entity-to-object converter
     * @param <V>        MessagePack entity type
     * @param <O>        object type
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(Class<? extends V> valueClass,
                                                            ValueConverter<V, ? extends O> converter) {
        try {
            Class<O> objectClass = getInterfaceParameterClass(converter, ValueConverter.class, 1);
            registerValueConverter(valueClass, objectClass, converter);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            throw new TarantoolClientException("Failed to determine the target parameter type of the generic " +
                    "interface, try to use the method registerValueConverter(valueClass, objectClass, converter) " +
                    "for registering the converter");
        }
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<? extends V> valueClass,
                                                            Class<? extends O> objectClass,
                                                            ValueConverter<V, ? extends O> converter) {
        List<ValueConverter<? extends Value, ?>> converters =
                valueConverters.computeIfAbsent(valueClass.getTypeName(), k -> new LinkedList<>());
        converters.add(0, converter);
        valueConvertersByTarget.put(objectClass.getTypeName(), converter);
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkConverterByTargetType(ValueConverter<? extends Value, ?> converter, Class<?> targetClass) {
        try {
            ValueConverter<? extends Value, ?> exactMatch = valueConvertersByTarget.get(targetClass.getTypeName());
            return exactMatch == converter ||
                    getInterfaceParameterClass(converter, converter.getClass(), 1).isAssignableFrom(targetClass);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            return false;
        }
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkObjectConverterByTargetType(ObjectConverter<?, ? extends Value> converter,
                                                     Class<?> targetClass) {
        try {
            return objectConvertersByTarget.get(targetClass.getTypeName()) == converter ||
                    getInterfaceParameterClass(converter, converter.getClass(), 1)
                            .isAssignableFrom(targetClass);
        } catch (InterfaceParameterClassNotFoundException | InterfaceParameterTypeNotFoundException e) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> targetClass) {
        Function<String, Optional<ValueConverter<V, O>>> getter =
                typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .filter(c -> checkConverterByTargetType(c, targetClass))
                        .map(c -> (ValueConverter<V, O>) c)
                        .findFirst();
        return findValueConverter(entityClass, getter);
    }

    /**
     * Perform {@link ObjectConverter} converter registration. The source object class and target entity class for
     * registration are determined automatically
     *
     * @param converter object-to-entity converter
     * @param <V>       MessagePack entity type
     * @param <O>       object type
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
    public <V extends Value, O> void registerObjectConverter(Class<? extends O> objectClass, ObjectConverter<O, V> converter) {
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
    public <V extends Value, O> void registerObjectConverter(Class<? extends O> objectClass,
                                                             Class<V> valueClass,
                                                             ObjectConverter<O, V> converter) {
        List<ObjectConverter<?, ? extends Value>> converters =
                objectConverters.computeIfAbsent(objectClass.getTypeName(), k -> new LinkedList<>());
        converters.add(0, converter);
        objectConvertersByTarget.put(valueClass.getTypeName(), converter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ObjectConverter<O, V>> getObjectConverter(Class<O> objectClass,
                                                                                   Class<V> valueClass) {
        Function<String, Optional<ObjectConverter<O, V>>> getter =
                typeName -> objectConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .filter(c -> checkObjectConverterByTargetType(c, valueClass))
                        .map(c -> (ObjectConverter<O, V>) c)
                        .findFirst();
        return findObjectConverter(objectClass, getter);
    }

    /**
     * Convenience method for registering classes implementing both types of converters.
     *
     * @param converter   object-to-entity and entity-to-object converter
     * @param valueClass  entity class
     * @param objectClass object class
     * @param <V>         MessagePack entity type
     * @param <O>         object type
     * @param <T>         converter type
     */
    public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> void registerConverter(
            Class<V> valueClass, Class<O> objectClass, T converter) {
        registerValueConverter(valueClass, objectClass, converter);
        registerObjectConverter(objectClass, valueClass, converter);
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
            mapper.registerValueConverter(ImmutableMapValueImpl.class, new DefaultMapValueToMapConverter(mapper));
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
            mapper.registerValueConverter(ImmutableArrayValueImpl.class, new DefaultArrayValueToListConverter(mapper));
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
        public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> Builder withConverter(
                Class<V> valueClass, Class<O> objectClass, T converter) {
            mapper.registerConverter(valueClass, objectClass, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withValueConverter(ValueConverter<V, O> converter) {
            mapper.registerValueConverter(converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueClass, converter);
            return this;
        }

        @Override
        public <V extends Value, O> Builder withValueConverter(Class<? extends V> valueClass, Class<O> objectClass,
                                                               ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueClass, objectClass, converter);
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
        public <V extends Value, O> Builder withObjectConverter(Class<O> objectClass, Class<V> valueClass,
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
