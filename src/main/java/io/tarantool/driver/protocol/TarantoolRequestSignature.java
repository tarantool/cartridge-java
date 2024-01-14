package io.tarantool.driver.protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import org.msgpack.value.Value;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.converters.ValueConverter;

/**
 * Represents a request signature, uniquely defining the operation and the
 * argument types. May include some argument values as well.
 *
 * The hashcode calculation is not thread safe.
 *
 * @author Alexey Kuzin
 */
public final class TarantoolRequestSignature {

    private List<String> components = new ArrayList<>();
    private int hashCode = 1;

    /**
     * Constructor.
     *
     * Creates an empty signature - do not use it without providing the components!
     */
    public TarantoolRequestSignature() {
    }

    /**
     * Constructor.
     *
     * Stores either the component values if the component is of type String or the
     * class names and calculates the hashcode from the passed initial set of
     * components.
     *
     * @param initialComponents initial signature components
     */
    private TarantoolRequestSignature(Object[] initialComponents) {
        for (Object component : initialComponents) {
            if (component == null) {
                continue;
            }
            String componentValue = component instanceof String ? (String) component : component.getClass().getName();
            components.add(componentValue);
            hashCode = 31 * hashCode + Objects.hashCode(componentValue);
        }
    }

    /**
     * Add a signature component to the end of the components list
     *
     * Appends either the component value if the component is of type String or the
     * component class to the components list and re-calculates the hashcode.
     *
     * @param component signature component
     * @return this signature object instance
     */
    public TarantoolRequestSignature addComponent(Object component) {
        if (component != null) {
            String componentValue = component instanceof String ? (String) component : component.getClass().getName();
            components.add(componentValue);
            hashCode = 31 * hashCode + Objects.hashCode(componentValue);
        }
        return this;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TarantoolRequestSignature
                && Objects.equals(this.components, ((TarantoolRequestSignature) other).components);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (Object component : components) {
            sb.append(String.valueOf(component)).append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Factory method for building the common signature part
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @return new request signature
     */
    private static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier) {
        Object[] components = new Object[arguments.size() + 2];
        int i = 0;
        components[i++] = functionName;
        for (Object argument : arguments) {
            components[i++] = argument.getClass().getName();
        }
        components[i++] = Integer.toHexString(argumentsMapperSupplier.hashCode());
        return new TarantoolRequestSignature(components);
    }

    /**
     * Factory method for caching default result mapper suppliers
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @param resultClass             type of the expected result. It's necessary
     *                                for polymorphic functions, e.g. accepting a
     *                                Tarantool space as an argument
     * @return new request signature
     */
    public static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier, Class<?> resultClass) {
        return TarantoolRequestSignature.create(functionName, arguments, argumentsMapperSupplier)
            .addComponent(resultClass.getName());
    }

    /**
     * Factory method for caching default result mapper suppliers
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @param valueConverter          single value result converter
     * @return new request signature
     */
    public static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
            ValueConverter<Value, ?> valueConverter) {
        return TarantoolRequestSignature.create(functionName, arguments, argumentsMapperSupplier)
            .addComponent(Integer.toHexString(valueConverter.hashCode()));
    }

    /**
     * Factory method for caching default multi value result mapper suppliers
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @param resultContainerSupplier multi value result container supplier
     * @param valueConverter          multi value result container item converter
     * @return new request signature
     */
    public static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
            Supplier<?> resultContainerSupplier, ValueConverter<Value, ?> valueConverter) {
        return TarantoolRequestSignature.create(functionName, arguments, argumentsMapperSupplier)
            .addComponent(Integer.toHexString(resultContainerSupplier.hashCode()))
            .addComponent(Integer.toHexString(valueConverter.hashCode()));
    }

    /**
     * Factory method for caching default multi value result mapper suppliers
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @param resultContainerSupplier multi value result container supplier
     * @param resultClass             multi value result item class
     * @return new request signature
     */
    public static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
            Supplier<?> resultContainerSupplier, Class<?> resultClass) {
        return TarantoolRequestSignature.create(functionName, arguments, argumentsMapperSupplier)
            .addComponent(Integer.toHexString(resultContainerSupplier.hashCode()))
            .addComponent(resultClass.getName());
    }

    /**
     * Factory method for a typical RPC usage
     *
     * @param functionName            name of the remote function
     * @param arguments               list of arguments for the remote function
     * @param argumentsMapperSupplier arguments mapper supplier
     * @param resultMapperSupplier    result mapper supplier
     * @return new request signature
     */
    public static TarantoolRequestSignature create(String functionName, Collection<?> arguments,
            Supplier<? extends MessagePackObjectMapper> argumentsMapperSupplier,
            Supplier<? extends MessagePackValueMapper> resultMapperSupplier) {
        return TarantoolRequestSignature.create(functionName, arguments, argumentsMapperSupplier)
            .addComponent(Integer.toHexString(resultMapperSupplier.hashCode()));
    }
}
