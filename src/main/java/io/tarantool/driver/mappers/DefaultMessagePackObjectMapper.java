package io.tarantool.driver.mappers;

import org.msgpack.value.Value;
import org.springframework.lang.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Default implementation of {@link MessagePackObjectMapper}. Deals with standard Java objects
 *
 * @author Alexey Kuzin
 */
public class DefaultMessagePackObjectMapper implements MessagePackObjectMapper {

    private static DefaultMessagePackObjectMapper instance;

    private Map<String, List<ValueConverter<? extends Value, ?>>> valueConverters;
    private Map<String, List<ObjectConverter<?, ? extends Value>>> objectConverters;

    public DefaultMessagePackObjectMapper() {
        valueConverters = new HashMap<>();
        objectConverters = new HashMap<>();
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
                        .filter((c) -> ((ValueConverter<V, O>) c).canConvertValue(v)).findFirst();
        Optional<ValueConverter<? extends Value, ?>> converter = findConverter(v.getClass(), getter);
        if (!converter.isPresent()) {
            throw new MessagePackValueMapperException("ValueConverter for type %s is not found", v.getClass());
        }
        return ((ValueConverter<V, O>) converter.get()).fromValue(v);
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
     * Perform {@link ValueConverter} converter registration.
     * @param valueClass entity class to register the converter for
     * @param converter object-to-entity converter
     * @see ValueConverter
     */
    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, ValueConverter<V, O> converter) {
        List<ValueConverter<? extends Value, ?>> converters = valueConverters.computeIfAbsent(valueClass.getTypeName(), (k) -> new LinkedList<>());
        converters.add(converter);
    }

    /**
     * Perform {@link ValueConverter} converter registration. The entity class for registration is determined automatically
     * @param converter entity-to-object converter
     * @see ValueConverter
     */
    public <V extends Value, O> void registerValueConverter(ValueConverter<V, O> converter) {
        String typeName = getInterfaceFirstParameterType(converter, ValueConverter.class).getTypeName();
        List<ValueConverter<? extends Value, ?>> converters = valueConverters.computeIfAbsent(typeName, (k) -> new LinkedList<>());
        converters.add(converter);
    }

    /**
     * Perform {@link ObjectConverter} converter registration.
     * @param objectClass object class to register the converter for
     * @param converter entity-to-object converter
     */
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
        String typeName = getInterfaceFirstParameterType(converter, ObjectConverter.class).getTypeName();
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

    private Type getInterfaceFirstParameterType(Object converter, Class<?> interfaceClass) {
        Type[] genericInterfaces = converter.getClass().getGenericInterfaces();
        if (genericInterfaces == null) {
            throw new RuntimeException(String.format("Unable to determine the generic interfaces for %s", converter.getClass()));
        }
        Type parameterType = null;
        try {
            for (Type iface : genericInterfaces) {
                ParameterizedType parameterizedType = (ParameterizedType) iface;
                if (Class.forName(parameterizedType.getRawType().getTypeName()).equals(interfaceClass)) {
                    Type[] typeParams = parameterizedType.getActualTypeArguments();
                    if (typeParams == null || typeParams.length != 2) {
                        throw new RuntimeException(String.format("Failed to get the interface %s generic parameters for %s", interfaceClass, converter.getClass()));
                    }
                    if (typeParams[0] instanceof ParameterizedType &&
                            Stream.of(((ParameterizedType) typeParams[0]).getActualTypeArguments()).allMatch(t -> t instanceof WildcardType)) {
                        parameterType = ((ParameterizedType) typeParams[0]).getRawType();
                    } else {
                        parameterType = typeParams[0];
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (parameterType == null) {
            throw new RuntimeException(String.format("Unable to determine the generic parameter type for %s", converter.getClass()));
        }
        return parameterType;
    }

    public static DefaultMessagePackObjectMapper getInstance() {
        if (instance == null) {
            instance = new DefaultMessagePackObjectMapper.Builder()
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
            instance.registerConverter(new DefaultListConverter(instance));
            // allow recursive map unpacking
            instance.registerConverter(new DefaultMapConverter(instance));
            // internal types converter
            instance.registerObjectConverter(new DefaultPackableObjectConverter(instance));
        }
        return instance;
    }

    /**
     * Builder for {@link DefaultMessagePackObjectMapper}
     */
    public static class Builder {
        private final DefaultMessagePackObjectMapper mapper;

        public Builder() {
            mapper = new DefaultMessagePackObjectMapper();
        }

        public Builder(DefaultMessagePackObjectMapper mapper) {
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

        public DefaultMessagePackObjectMapper build() {
            return mapper;
        }
    }
}
