package io.tarantool.driver.mappers;

import org.msgpack.value.Value;
import org.springframework.lang.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.tarantool.driver.mappers.MapperReflectionUtils.getConverterTargetType;
import static io.tarantool.driver.mappers.MapperReflectionUtils.getInterfaceParameterClass;
import static io.tarantool.driver.mappers.MapperReflectionUtils.getInterfaceParameterType;

/**
 * Default implementation of {@link MessagePackObjectMapper} and {@link MessagePackValueMapper}. Deals with standard Java objects
 *
 * @author Alexey Kuzin
 */
public class DefaultMessagePackMapper implements MessagePackObjectMapper, MessagePackValueMapper {

    private static DefaultMessagePackMapper instance;

    private Map<String, List<ValueConverter<? extends Value, ?>>> valueConverters;
    private Map<String, List<ObjectConverter<?, ? extends Value>>> objectConverters;
    private Map<String, ValueConverter<? extends Value, ?>> valueConvertersByTarget;

    public DefaultMessagePackMapper() {
        valueConverters = new HashMap<>();
        valueConvertersByTarget = new HashMap<>();
        objectConverters = new HashMap<>();
    }

    private DefaultMessagePackMapper(Map<String, List<ValueConverter<? extends Value, ?>>> valueConverters, Map<String, List<ObjectConverter<?, ? extends Value>>> objectConverters, Map<String, ValueConverter<? extends Value, ?>> valueConvertersByTarget) {
        this.valueConverters = valueConverters;
        this.objectConverters = objectConverters;
        this.valueConvertersByTarget = valueConvertersByTarget;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> V toValue(O o) {
        Function<String, Optional<ObjectConverter<?, ? extends Value>>> getter =
                (typeName) -> objectConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .filter((c) -> ((ObjectConverter<O, V>) c).canConvertObject(o)).findFirst();
        Optional<ObjectConverter<?, ? extends Value>> converter = findConverter(o.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ObjectConverter for type %s is not found", o.getClass());
        }
        return ((ObjectConverter<O, V>) converter.get()).toValue(o);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> O fromValue(V v) {
        Function<String, Optional<ValueConverter<? extends Value, ?>>> getter =
                (typeName) -> valueConverters.getOrDefault(typeName, Collections.emptyList()).stream()
                        .filter((c) -> checkTargetType(c, v) && ((ValueConverter<V, O>) c).canConvertValue(v))
                        .findFirst();
        Optional<ValueConverter<? extends Value, ?>> converter = findConverter(v.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ValueConverter for type %s is not found", v.getClass());
        }
        return ((ValueConverter<V, O>) converter.get()).fromValue(v);
    }

    /**
     * Check if the specified converter can convert to the specified object type
     */
    private boolean checkTargetType(ValueConverter<? extends Value, ?> converter, Object value) {
        return getInterfaceParameterClass(converter, converter.getClass(), 1).isAssignableFrom(value.getClass());
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

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
        Class<O> objectClass = getConverterTargetType(converter);
        registerValueConverter(valueClass, objectClass, converter);
    }

    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, Class<O> objectClass, ValueConverter<V, O> converter) {
        List<ValueConverter<? extends Value, ?>> converters = valueConverters.computeIfAbsent(valueClass.getTypeName(), (k) -> new LinkedList<>());
        converters.add(converter);
        valueConvertersByTarget.put(objectClass.getTypeName(), converter);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<O> objectClass) {
        return Optional.ofNullable((ValueConverter<V, O>) valueConvertersByTarget.get(objectClass.getTypeName()));
    }

    /**
     * Perform {@link ValueConverter} converter registration. The entity class for registration is determined automatically
     * @param converter entity-to-object converter
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(ValueConverter<V, O> converter) {
        String typeName = getInterfaceParameterType(converter, ValueConverter.class, 0).getTypeName();
        List<ValueConverter<? extends Value, ?>> converters = valueConverters.computeIfAbsent(typeName, (k) -> new LinkedList<>());
        converters.add(converter);
    }

    @Override
    public <V extends Value, O> void registerObjectConverter(Class<O> objectClass, ObjectConverter<O, V> converter) {
        List<ObjectConverter<?, ? extends Value>> converters = objectConverters.computeIfAbsent(objectClass.getTypeName(), (k) -> new LinkedList<>());
        converters.add(converter);
    }

    /**
     * Perform {@link ObjectConverter} converter registration. The object class for registration is determined automatically
     * @param converter object-to-entity converter
     * @see ObjectConverter
     */
    public <V extends Value, O> void registerObjectConverter(ObjectConverter<O, V> converter) {
        String typeName = getInterfaceParameterType(converter, ObjectConverter.class, 0).getTypeName();
        List<ObjectConverter<?, ? extends Value>> converters = objectConverters.computeIfAbsent(typeName, (k) -> new LinkedList<>());
        converters.add(converter);
    }

    /**
     * Convenience method for registering classes implementing both types of converters.
     * @param converter object-to-entity and entity-to-object converter
     */
    public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> void registerConverter(T converter) {
        registerValueConverter(converter);
        registerObjectConverter(converter);
    }

    public static DefaultMessagePackMapper getInstance() {
        if (instance == null) {
            instance = new DefaultMessagePackMapper.Builder()
                    .withConverter(new DefaultStringConverter())
                    .withConverter(new DefaultIntegerConverter())
                    .withConverter(new DefaultLongConverter())
                    .withConverter(new DefaultByteArrayConverter())
                    //TODO add converters:
                    //TODO boolean
                    //TODO float
                    //TODO double
                    //TODO decimal
                    //TODO UUID
                    .build();
            // allow recursive list unpacking
            instance.registerObjectConverter(new DefaultListObjectConverter(instance));
            instance.registerValueConverter(new DefaultListValueConverter(instance));
            // allow recursive map unpacking
            instance.registerObjectConverter(new DefaultMapObjectConverter(instance));
            instance.registerValueConverter(new DefaultMapValueConverter(instance));
            // internal types converter
            instance.registerObjectConverter(new DefaultPackableObjectConverter(instance));
        }
        return instance;
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        Map<String, List<ValueConverter<? extends Value, ?>>> valueConvertersClone = new HashMap<>(valueConverters);
        Map<String, List<ObjectConverter<?, ? extends Value>>> objectConvertersClone = new HashMap<>(objectConverters);
        Map<String, ValueConverter<? extends Value, ?>> valueConvertersByTargetClone = new HashMap<>(valueConvertersByTarget);
        DefaultMessagePackMapper mapper = new DefaultMessagePackMapper(valueConvertersClone, objectConvertersClone, valueConvertersByTargetClone);
        return mapper;
    }

    /**
     * Builder for {@link DefaultMessagePackMapper}
     */
    public static class Builder {
        private final DefaultMessagePackMapper mapper;

        public Builder() {
            mapper = new DefaultMessagePackMapper();
        }

        public Builder(DefaultMessagePackMapper mapper) {
            this.mapper = mapper;
        }

        public <V extends Value, O, T extends ValueConverter<V, O> & ObjectConverter<O, V>> Builder withConverter(T converter) {
            mapper.registerValueConverter(converter);
            mapper.registerObjectConverter(converter);
            return this;
        }

        public <V extends Value, O> Builder withValueConverter(ValueConverter<V, O> converter) {
            mapper.registerValueConverter(converter);
            return this;
        }

        public <V, O extends Value> Builder withObjectConverter(ObjectConverter<V, O> converter) {
            mapper.registerObjectConverter(converter);
            return this;
        }

        public DefaultMessagePackMapper build() {
            return mapper;
        }
    }
}
