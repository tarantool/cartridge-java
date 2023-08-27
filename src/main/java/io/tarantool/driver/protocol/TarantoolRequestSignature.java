package io.tarantool.driver.protocol;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a request signature, uniquely defining the operation and the argument types.
 * May include some argument values as well.
 *
 * The hashcode calculation is not thread safe.
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestSignature {

    private List<String> components = new LinkedList<>();
    private int hashCode = 1;

    /**
     * Constructor.
     *
     * Stores either the component values if the component is of type String or the class names
     * and calculates the hashcode from the passed initial set of components.
     *
     * @param initialComponents initial signature components
     */
    public TarantoolRequestSignature(Object... initialComponents) {
        for (Object component: initialComponents) {
            String componentValue = component instanceof String ? (String) component : component.getClass().getName();
            components.add(componentValue);
            hashCode = 31 * hashCode + Objects.hashCode(componentValue);
        }
    }

    /**
     * Add a signature component to the end of the components list
     *
     * Appends either the component value if the component is of type String or the component class
     * to the components list and re-calculates the hashcode.
     *
     * @param component signature component
     * @return this signature object instance
     */
    public TarantoolRequestSignature addComponent(Object component) {
        String componentValue = component instanceof String ? (String) component : component.getClass().getName();
        components.add(componentValue);
        hashCode = 31 * hashCode + Objects.hashCode(componentValue);
        return this;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TarantoolRequestSignature &&
            Objects.equals(this.components, ((TarantoolRequestSignature) other).components);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (Object component: components) {
            sb.append(String.valueOf(component)).append(",");
        }
        sb.append("]");
        return sb.toString();
    }
}
