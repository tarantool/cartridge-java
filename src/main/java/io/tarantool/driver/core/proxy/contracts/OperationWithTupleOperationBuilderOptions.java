package io.tarantool.driver.core.proxy.contracts;

import io.tarantool.driver.api.space.options.interfaces.Self;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.core.proxy.interfaces.BuilderOptions;

public interface OperationWithTupleOperationBuilderOptions<T extends OperationWithTupleOperationBuilderOptions<T>>
    extends BuilderOptions, Self<T> {

    default T withTupleOperation(TupleOperations operations) {
        addArgument(ProxyOperationArgument.TUPLE_OPERATIONS, operations.asProxyOperationList());
        return self();
    }
}
