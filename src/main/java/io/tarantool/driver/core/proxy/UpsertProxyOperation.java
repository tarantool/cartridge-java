package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.interfaces.UpsertOptions;
import io.tarantool.driver.core.proxy.contracts.OperationWithTupleBuilderOptions;
import io.tarantool.driver.core.proxy.contracts.OperationWithTupleOperationBuilderOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Proxy operation for upsert
 *
 * @param <T> result type
 * @param <R> result collection type
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class UpsertProxyOperation<T extends Packable, R extends Collection<T>> extends AbstractProxyOperation<R> {

    UpsertProxyOperation(
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
        extends GenericOperationsBuilder<R, UpsertOptions<?>, Builder<T, R>> implements
        OperationWithTupleOperationBuilderOptions<Builder<T, R>>, OperationWithTupleBuilderOptions<Builder<T, R>, T> {

        public Builder() {
        }

        @Override
        public Builder<T, R> self() {
            return this;
        }

        public UpsertProxyOperation<T, R> build() {

            return new UpsertProxyOperation<>(
                this.client, this.functionName, new ArrayList<>(arguments.values()), this.argumentsMapper,
                this.resultMapper);
        }
    }
}
