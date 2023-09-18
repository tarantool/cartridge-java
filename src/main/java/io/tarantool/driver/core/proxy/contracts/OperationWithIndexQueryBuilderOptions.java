package io.tarantool.driver.core.proxy.contracts;

import io.tarantool.driver.api.space.options.interfaces.Self;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.core.proxy.interfaces.BuilderOptions;
import io.tarantool.driver.protocol.TarantoolIndexQuery;

public interface OperationWithIndexQueryBuilderOptions<T extends OperationWithIndexQueryBuilderOptions<T>>
    extends BuilderOptions, Self<T> {

    default T withIndexQuery(TarantoolIndexQuery indexQuery) {
        addArgument(ProxyOperationArgument.INDEX_QUERY, indexQuery.getKeyValues());
        return self();
    }
}
