package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.interfaces.InsertManyOptions;
import io.tarantool.driver.core.proxy.contracts.OperationWithTuplesBuilderOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Proxy operation for inserting many records at once
 *
 * @param <T> result type
 * @param <R> result collection type
 * @author Alexey Kuzin
 */
public final class InsertManyProxyOperation<T extends Packable, R extends Collection<T>>
    extends AbstractProxyOperation<R> {

    InsertManyProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        List<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        CallResultMapper<R, SingleValueCallResult<R>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T extends Packable, R extends Collection<T>>
        extends GenericOperationsBuilder<R, InsertManyOptions<?>, Builder<T, R>> implements
        OperationWithTuplesBuilderOptions<Builder<T, R>, T> {

        public Builder() {
        }

        @Override
        public Builder<T, R> self() {
            return this;
        }

        public InsertManyProxyOperation<T, R> build() {
            return new InsertManyProxyOperation<>(
                this.client, this.functionName, new ArrayList<>(arguments.values()), this.argumentsMapper,
                this.resultMapper);
        }
    }
}
