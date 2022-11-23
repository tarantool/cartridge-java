package io.tarantool.driver.mappers;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.stream.Stream;

/**
 * Contains helper methods for converter classes lookup and determining its parameters at runtime
 */
final class MapperReflectionUtils {

    private MapperReflectionUtils() {
    }

    /**
     * Get class for the runtime target type parameter of a converter
     *
     * @param converter a converter, must have at least one generic interface with 2 type parameters
     * @param <T>       the target converter type
     * @return the converter class
     * @throws InterfaceParameterClassNotFoundException if the class of the parameter type on the specified position
     *                                                  cannot be determined or is not found
     */
    @SuppressWarnings("unchecked")
    static <T> Class<T> getConverterTargetType(Object converter) throws InterfaceParameterClassNotFoundException {
        Type[] genericInterfaces = getGenericInterfaces(converter);
        if (genericInterfaces.length < 1) {
            throw new RuntimeException(
                String.format("The passed converter object of type %s does not extend any generic interface",
                    converter.getClass()));
        }
        if (genericInterfaces.length > 1) {
            throw new RuntimeException(
                String.format("The passed converter object of type %s has more than one generic interfaces, " +
                    "unable to determine the target", converter.getClass()));
        }
        if (!(genericInterfaces[0] instanceof ParameterizedType)) {
            throw new RuntimeException(
                String.format("The passed converter object of type %s interface type is not a parameterized type",
                    converter.getClass()));
        }
        try {
            return (Class<T>) Class.forName(
                ((ParameterizedType) genericInterfaces[0]).getActualTypeArguments()[1].getTypeName());
        } catch (ClassNotFoundException e) {
            throw new InterfaceParameterClassNotFoundException(e);
        }
    }

    /**
     * Call {@link #getInterfaceParameterType(Object, Class, int)} and get a class for the returned type
     *
     * @param converter             a converter, must have at least one generic interface with 2 type parameters
     * @param interfaceClass        the target converter generic interface
     * @param parameterTypePosition the position of the generic type parameter in the interface definition
     * @param <T>                   the target generic interface parameter type
     * @return the generic interface parameter class
     * @throws InterfaceParameterTypeNotFoundException  if the parameter type on the specified position cannot be
     *                                                  determined
     * @throws InterfaceParameterClassNotFoundException if the class of the parameter type on the specified position
     *                                                  cannot be determined or is not found
     */
    @SuppressWarnings("unchecked")
    static <T> Class<T> getInterfaceParameterClass(
        Object converter,
        Class<?> interfaceClass,
        int parameterTypePosition)
        throws InterfaceParameterTypeNotFoundException, InterfaceParameterClassNotFoundException {
        try {
            return (Class<T>) Class.forName(
                getInterfaceParameterType(converter, interfaceClass, parameterTypePosition).getTypeName());
        } catch (ClassNotFoundException e) {
            throw new InterfaceParameterClassNotFoundException(e);
        }
    }

    /**
     * <pre>{@code
     * SomeClass implements ValueConverter<V, O>, ObjectConverter<O, V>
     * <- interfaceClass: ValueConverter, parameterTypePosition: 0
     * -> V
     * }</pre>
     *
     * @param converter             a converter, must have at least one generic interface with 2 type parameters
     * @param interfaceClass        the target converter generic interface
     * @param parameterTypePosition the position of the generic type parameter in the interface definition
     * @return the generic interface parameter type
     * @throws InterfaceParameterTypeNotFoundException  if the parameter type on the specified position cannot be
     *                                                  determined
     * @throws InterfaceParameterClassNotFoundException if the class of the parameter type on the specified position
     *                                                  cannot be determined or is not found
     */
    private static Type getInterfaceParameterType(
        Object converter,
        Class<?> interfaceClass,
        int parameterTypePosition)
        throws InterfaceParameterTypeNotFoundException, InterfaceParameterClassNotFoundException {
        Type[] genericInterfaces = getGenericInterfaces(converter);
        try {
            for (Type iface : genericInterfaces) {
                if (iface instanceof ParameterizedType) {
                    ParameterizedType parameterizedType = (ParameterizedType) iface;
                    if (Class.forName(parameterizedType.getRawType().getTypeName()).isAssignableFrom(interfaceClass)) {
                        return getParameterType(parameterizedType, parameterTypePosition);
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            throw new InterfaceParameterClassNotFoundException(e);
        }
        throw new InterfaceParameterTypeNotFoundException(
            "Unable to determine the generic parameter type on position %d for %s. " +
                "Either the class does not implement any generic interfaces or the parametrized types " +
                "cannot be determined due to type erasure", parameterTypePosition, converter.getClass());
    }

    private static Type[] getGenericInterfaces(Object converter) {
        Type[] genericInterfaces = converter.getClass().getGenericInterfaces();
        if (genericInterfaces == null) {
            throw new RuntimeException(
                String.format("Unable to determine the generic interfaces for %s", converter.getClass()));
        }
        return genericInterfaces;
    }

    /**
     * <pre>{@code
     * ValueConverter<V, O>
     * <- parameterTypePosition: 0
     * -> V
     * }</pre>
     *
     * @param parameterizedType     a type of the generic interface parameter type
     * @param parameterTypePosition the position of the generic type parameter in the interface definition
     * @return the generic interface parameter type
     */
    private static Type getParameterType(ParameterizedType parameterizedType, int parameterTypePosition) {
        Type[] typeParams = parameterizedType.getActualTypeArguments();
        if (typeParams == null || typeParams.length != 2) {
            throw new RuntimeException(String.format("Type %s does not have 2 type parameters", parameterizedType));
        }
        if (typeParams[parameterTypePosition] instanceof ParameterizedType &&
            Stream.of(((ParameterizedType) typeParams[parameterTypePosition]).getActualTypeArguments())
                .allMatch(t -> t instanceof WildcardType)) {
            return ((ParameterizedType) typeParams[parameterTypePosition]).getRawType();
        } else {
            return typeParams[parameterTypePosition];
        }
    }
}
