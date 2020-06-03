package io.tarantool.driver.mappers;

import org.msgpack.value.Value;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.stream.Stream;

/**
 * Contains helper methods for converter classes lookup and determining its parameters at runtime
 */
public class MapperReflectionUtils {

    /**
     * Get class for the runtime target type parameter of a converter
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getConverterTargetType(Object converter) {
        Type[] genericInterfaces = getGenericInterfaces(converter);
        if (genericInterfaces.length < 1) {
            throw new RuntimeException(String.format("The passed converter object of type %s does not extend any generic interface", converter.getClass()));
        }
        try {
            Class<?> interfaceClass = Class.forName(genericInterfaces[0].getTypeName());
            return (Class<T>) Class.forName(getInterfaceParameterType(converter, interfaceClass, 1).getTypeName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Call {@link #getInterfaceParameterType(Object, Class, int)} and get a class for the returned type
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getInterfaceParameterClass(Object converter, Class<?> interfaceClass, int parameterTypePosition) {
        try {
            return (Class<T>) Class.forName(getInterfaceParameterType(converter, interfaceClass, parameterTypePosition).getTypeName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@code SomeClass implements ValueConverter<V, O>, ObjectConverter<O, V>}
     * <- {@code interfaceClass: ValueConverter, parameterTypePosition: 0 }
     * -> {@code V}
     */
    public static Type getInterfaceParameterType(Object converter, Class<?> interfaceClass, int parameterTypePosition) {
        Type[] genericInterfaces = getGenericInterfaces(converter);
        try {
            for (Type iface : genericInterfaces) {
                ParameterizedType parameterizedType = (ParameterizedType) iface;
                if (Class.forName(parameterizedType.getRawType().getTypeName()).isAssignableFrom(interfaceClass)) {
                    return getParameterType(parameterizedType, parameterTypePosition);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException(String.format("Unable to determine the generic parameter type for %s", converter.getClass()));
    }

    private static Type[] getGenericInterfaces(Object converter) {
        Type[] genericInterfaces = converter.getClass().getGenericInterfaces();
        if (genericInterfaces == null) {
            throw new RuntimeException(String.format("Unable to determine the generic interfaces for %s", converter.getClass()));
        }
        return genericInterfaces;
    }

    /**
     * {@code ValueConverter<V, O>}
     * <- {@code parameterTypePosition: 0 }
     * -> {@code V}
     */
    public static Type getParameterType(ParameterizedType parameterizedType, int parameterTypePosition) {
        Type[] typeParams = parameterizedType.getActualTypeArguments();
        if (typeParams == null || typeParams.length != 2) {
            throw new RuntimeException(String.format("Type %s does not have 2 type parameters", parameterizedType));
        }
        if (typeParams[parameterTypePosition] instanceof ParameterizedType &&
                Stream.of(((ParameterizedType) typeParams[parameterTypePosition]).getActualTypeArguments()).allMatch(t -> t instanceof WildcardType)) {
            return ((ParameterizedType) typeParams[parameterTypePosition]).getRawType();
        } else {
            return typeParams[parameterTypePosition];
        }
    }
}
