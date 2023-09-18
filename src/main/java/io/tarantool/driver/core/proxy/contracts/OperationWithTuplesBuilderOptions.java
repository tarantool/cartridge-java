package io.tarantool.driver.core.proxy.contracts;

import io.tarantool.driver.api.space.options.interfaces.Self;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.core.proxy.interfaces.BuilderOptions;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.Objects;

public interface OperationWithTuplesBuilderOptions<T extends OperationWithTuplesBuilderOptions<T, P>,
    P extends Packable> extends BuilderOptions, Self<T> {

    default T withTuples(Collection<P> tuples) {
        if (Objects.isNull(tuples)) {
            throw new IllegalArgumentException("Tuples must be specified for batch insert operation");
        }
        addArgument(ProxyOperationArgument.TUPLE, tuples);
        return self();
    }
}
