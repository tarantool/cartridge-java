package io.tarantool.driver.core.proxy.contracts;

import io.tarantool.driver.api.space.options.interfaces.Self;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.core.proxy.interfaces.BuilderOptions;
import io.tarantool.driver.protocol.Packable;

public interface OperationWithTupleBuilderOptions<T extends OperationWithTupleBuilderOptions<T, R>, R extends Packable>
    extends BuilderOptions, Self<T> {

    default T withTuple(R tuple) {
        addArgument(ProxyOperationArgument.TUPLE, tuple);
        return self();
    }
}
