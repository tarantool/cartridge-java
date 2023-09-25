package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.protocol.TarantoolIndexQuery;

public interface OperationWithIndexQueryBuilderOptions<T extends OperationWithIndexQueryBuilderOptions<T>>
    extends BuilderOptions, Self<T> {

    default T withIndexQuery(TarantoolIndexQuery indexQuery) {
        addArgument(ProxyOperationArgument.INDEX_QUERY, indexQuery.getKeyValues());
        return self();
    }
}
