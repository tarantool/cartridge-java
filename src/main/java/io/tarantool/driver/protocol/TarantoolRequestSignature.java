package io.tarantool.driver.protocol;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a request signature, uniquely defining the operation and the argument types.
 * May include some argument values as well.
 *
 * @author Alexey Kuzin
 */
public class TarantoolRequestSignature {

    private List<Object> components = new LinkedList<>();
    private int hashCode = 1;

    /**
     * Constructor.
     *
     * @param initialComponents initial signature components, must be hashable
     */
    public TarantoolRequestSignature(Object... initialComponents) {
        for (Object component: initialComponents) {
            components.add(component);
            hashCode = 31 * hashCode + Objects.hashCode(component);
        }
    }

    /**
     * Add a signature component to the end of the components list
     *
     * @param component signature component, must be hashable
     * @return this signature object instance
     */
    public TarantoolRequestSignature addComponent(Object component) {
        components.add(component);
        hashCode = 31 * hashCode + Objects.hashCode(component);
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
