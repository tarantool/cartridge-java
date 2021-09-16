package io.tarantool.driver.api.client;

import java.util.Objects;

public abstract class AbstractClientParameter<T> implements ClientParameter {

    private final T value;

    public AbstractClientParameter(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

    public abstract ClientParameterType getParameterType();

    @Override
    public int compareTo(AbstractClientParameter<?> o) {
        return this.getParameterType().getOrder() - o.getParameterType().getOrder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (getClass() == o.getClass()) return true;
        AbstractClientParameter<?> that = (AbstractClientParameter<?>) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.getClass().getName());
    }
}
