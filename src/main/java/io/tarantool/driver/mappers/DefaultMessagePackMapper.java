package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import org.msgpack.value.Value;
import org.springframework.lang.Nullable;

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
 */
public class DefaultMessagePackMapper implements MessagePackMapper {

    private Map<String, List<ValueConverter<? extends Value, ?>>> valueConverters;
    private Map<String, List<ObjectConverter<?, ? extends Value>>> objectConverters;
    private Map<String, ValueConverter<? extends Value, ?>> valueConvertersByTarget;
    private Map<String, ObjectConverter<?, ? extends Value>> objectConvertersByTarget;

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
     * @param mapper another mapper instance
     */
    public DefaultMessagePackMapper(DefaultMessagePackMapper mapper) {
        this();
        this.valueConverters.putAll(mapper.valueConverters);
        this.objectConverters.putAll(mapper.objectConverters);
        this.valueConvertersByTarget.putAll(mapper.valueConvertersByTarget);
        this.objectConvertersByTarget.putAll(mapper.objectConvertersByTarget);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> V toValue(O o) {
        Function<String, Optional<ObjectConverter<O, V>>> getter =
                typeName -> objectConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .map(c -> (ObjectConverter<O, V>) c)
                        .filter(c -> c.canConvertObject(o))
                        .findFirst();
        Optional<ObjectConverter<O, V>> converter = findConverter(o.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackObjectMapperException("ObjectConverter for type %s is not found", o.getClass());
        }
        return converter.get().toValue(o);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v) {
        Function<String, Optional<ValueConverter<V, O>>> getter =
                typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .map(c -> (ValueConverter<V, O>) c)
                        .filter(c -> c.canConvertValue(v))
                        .findFirst();
        Optional<ValueConverter<V, O>> converter = findConverter(v.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ValueConverter for type %s is not found", v.getClass());
        }
        return converter.get().fromValue(v);
    }

    @Nullable
    private <T> Optional<T> findConverter(Class<?> objectClass, Function<String, Optional<T>> getter) {
        Optional<T> converter = getter.apply(objectClass.getTypeName());
        if (!converter.isPresent() && objectClass.getSuperclass() != null) {
            converter = findConverter(objectClass.getSuperclass(), getter);
        }
        if (!converter.isPresent()) {
            for (Class<?> iface : objectClass.getInterfaces()) {
                converter = findConverter(iface, getter);
                if (converter.isPresent()) {
                    break;
                }
            }
        }
        return converter;
    }

    /**
     * Perform {@link ValueConverter} converter registration. The source entity class and target object class for
     * registration are determined automatically
     * @param converter entity-to-object converter
     * @param <V> MessagePack entity type
     * @param <O> object type
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(ValueConverter<V, O> converter) {
        try {
            registerValueConverter(
                    getInterfaceParameterClass(converter, ValueConverter.class, 0), converter);
        } catch (ConverterParameterTypeNotFoundException e) {
            throw new RuntimeException("Failed to determine the source parameter type of the generic interface, " +
                    "try to use the method registerValueConverter(valueClass, objectClass, converter) " +
                    "for registering the converter");
        }
    }

    /**
     * Perform {@link ValueConverter} converter registration. The target object class for registration is determined
     * automatically
     * @param valueClass source entity class
     * @param converter entity-to-object converter
     * @param <V> MessagePack entity type
     * @param <O> object type
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
        try {
            Class<O> objectClass = getInterfaceParameterClass(converter, ValueConverter.class, 1);
            registerValueConverter(valueClass, objectClass, converter);
        } catch (ConverterParameterTypeNotFoundException e) {
            throw new RuntimeException("Failed to determine the target parameter type of the generic interface, " +
                    "try to use the method registerValueConverter(valueClass, objectClass, converter) " +
                    "for registering the converter");
        }
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, Class<O> objectClass,
                                                            ValueConverter<V, O> converter) {
        List<ValueConverter<? extends Value, ?>> converters =
                valueConverters.computeIfAbsent(valueClass.getTypeName(), k -> new LinkedList<>());
        converters.add(converter);
        valueConvertersByTarget.put(objectClass.getTypeName(), converter);
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkTargetType(ValueConverter<? extends Value, ?> converter, Class<?> targetClass) {
        try {
            return valueConvertersByTarget.get(targetClass.getTypeName()) == converter ||
                    getInterfaceParameterClass(converter, converter.getClass(), 1)
                            .isAssignableFrom(targetClass);
        } catch (ConverterParameterTypeNotFoundException e) {
            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> targetClass) {
        Function<String, Optional<ValueConverter<V, O>>> getter =
                typeName -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .filter(c -> checkTargetType(c, targetClass))
                        .map(c -> (ValueConverter<V, O>) c)
                        .findFirst();
        return findConverter(entityClass, getter);
    }

    /**
     * Perform {@link ObjectConverter} converter registration. The source object class and target entity class for
     * registration are determined automatically
     * @param converter object-to-entity converter
     * @param <V> MessagePack entity type
     * @param <O> object type
     * @see ObjectConverter
     */
    public <V extends Value, O> void registerObjectConverter(ObjectConverter<O, V> converter) {
        try {
            registerObjectConverter(
                    getInterfaceParameterClass(converter, ObjectConverter.class, 0), converter);
        } catch (ConverterParameterTypeNotFoundException e) {
            throw new RuntimeException("Failed to determine the target parameter type of the generic interface, " +
                    "try to use the method registerObjectConverter(objectClass, valueClass, converter) " +
                    "for registering the converter");
        }
    }

    /**
     * Adds a Java object converter to this mappers instance. The target value class for registration is determined
     * automatically
     * @param objectClass object class to register the converter for
     * @param converter entity-to-object converter
     * @param <V> the target MessagePack entity type
     * @param <O> the source object type
     * @see ObjectConverter
     */
    public <V extends Value, O> void registerObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter) {
        try {
            Class<V> valueClass = getInterfaceParameterClass(converter, ObjectConverter.class, 1);
            registerObjectConverter(objectClass, valueClass, converter);
        } catch (ConverterParameterTypeNotFoundException e) {
            throw new RuntimeException("Failed to determine the target parameter type of the generic interface, " +
                    "try to use the method registerObjectConverter(objectClass, valueClass, converter) " +
                    "for registering the converter");
        }
    }

    @Override
    public <V extends Value, O> void registerObjectConverter(Class<O> objectClass, Class<V> valueClass,
                                                             ObjectConverter<O, V> converter) {
        List<ObjectConverter<?, ? extends Value>> converters =
                objectConverters.computeIfAbsent(objectClass.getTypeName(), k -> new LinkedList<>());
        converters.add(converter);
        objectConvertersByTarget.put(valueClass.getTypeName(), converter);
    }

    /**
     * Convenience method for registering classes implementing both types of converters.
     * @param converter object-to-entity and entity-to-object converter
     * @param valueClass entity class
     * @param objectClass object class
     * @param <V> MessagePack entity type
     * @param <O> object type
     * @param <T> converter type
     */
    public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> void registerConverter(
            Class<V> valueClass, Class<O> objectClass, T converter) {
        registerValueConverter(valueClass, objectClass, converter);
        registerObjectConverter(objectClass, valueClass, converter);
    }

    /**
     * Builder for {@link DefaultMessagePackMapper}
     */
    public static class Builder {
        private final DefaultMessagePackMapper mapper;

        /**
         * Basic constructor, initialized with an empty mapper
         */
        public Builder() {
            mapper = new DefaultMessagePackMapper();
        }

        /**
         * Basic constructor, initialized with the specified mapper
         * @param mapper a mapper instance
         */
        public Builder(DefaultMessagePackMapper mapper) {
            this.mapper = new DefaultMessagePackMapper(mapper);
        }

        /**
         * Configure the mapper with default {@code MP_MAP} entity to {@link Map} converter
         * @return builder
         */
        public Builder withDefaultMapValueConverter() {
            mapper.registerValueConverter(new DefaultMapValueConverter(mapper));
            return this;
        }

        /**
         * Configure the mapper with default {@link Map} to {@code MP_MAP} entity converter
         * @return builder
         */
        public Builder withDefaultMapObjectConverter() {
            mapper.registerObjectConverter(new DefaultMapObjectConverter(mapper));
            return this;
        }

        /**
         * Configure the mapper with default {@code MP_ARRAY} entity to {@link List} converter
         * @return builder
         */
        public Builder withDefaultArrayValueConverter() {
            mapper.registerValueConverter(new DefaultListValueConverter(mapper));
            return this;
        }

        /**
         * Configure the mapper with default {@link List} to {@code MP_ARRAY} entity converter
         * @return builder
         */
        public Builder withDefaultListObjectConverter() {
            mapper.registerObjectConverter(new DefaultListObjectConverter(mapper));
            return this;
        }

        /**
         * Configure the mapper with default {@code MP_ARRAY} entity to {@link TarantoolTuple} converter
         * @return builder
         */
        public Builder withDefaultArrayValueToTarantoolTupleConverter() {
            mapper.registerValueConverter(new DefaultArrayValueToTarantoolTupleConverter(mapper));
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack entity-to-object and object-to-entity converter
         * @param valueClass MessagePack entity class
         * @param objectClass object class
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @param <T> converter type
         * @return builder
         */
        public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> Builder withConverter(
                Class<V> valueClass, Class<O> objectClass, T converter) {
            mapper.registerConverter(valueClass, objectClass, converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack entity-to-object converter
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         * @see #registerValueConverter(ValueConverter)
         */
        public <V extends Value, O> Builder withValueConverter(ValueConverter<V, O> converter) {
            mapper.registerValueConverter(converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack entity-to-object converter
         * @param valueClass source entity class
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         * @see #registerValueConverter(Class, ValueConverter)
         */
        public <V extends Value, O> Builder withValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueClass, converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack entity-to-object converter
         * @param valueClass source entity class
         * @param objectClass target object class
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         * @see #registerValueConverter(Class, Class, ValueConverter)
         */
        public <V extends Value, O> Builder withValueConverter(Class<V> valueClass, Class<O> objectClass,
                                                               ValueConverter<V, O> converter) {
            mapper.registerValueConverter(valueClass, objectClass, converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack object-to-entity converter
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         */
        public <V extends Value, O> Builder withObjectConverter(ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack object-to-entity converter
         * @param objectClass source object class
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         */
        public <V extends Value, O> Builder withObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(objectClass, converter);
            return this;
        }

        /**
         * Configure the mapper with a specified MessagePack object-to-entity converter
         * @param objectClass source object class
         * @param valueClass target object class
         * @param converter MessagePack entity-to-object and object-to-entity converter
         * @param <V> MessagePack entity type
         * @param <O> object type
         * @return builder
         */
        public <V extends Value, O> Builder withObjectConverter(Class<O> objectClass, Class<V> valueClass,
                                                                ObjectConverter<O, V> converter) {
            mapper.registerObjectConverter(objectClass, valueClass, converter);
            return this;
        }

        /**
         * Build the mapper instance
         * @return a new mapper instance
         */
        public DefaultMessagePackMapper build() {
            return mapper;
        }
    }
}
