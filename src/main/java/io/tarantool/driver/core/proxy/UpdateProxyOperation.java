package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.UpdateOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Proxy operation for update
 *
 * @param <T> result type
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class UpdateProxyOperation<T> extends AbstractProxyOperation<T> {

    UpdateProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        List<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T>
        extends GenericOperationsBuilder<T, UpdateOptions<?>, Builder<T>> implements
        OperationWithIndexQueryBuilderOptions<Builder<T>>, OperationWithTupleOperationBuilderOptions<Builder<T>> {

        public Builder() {
        }

        @Override
        public Builder<T> self() {
            return this;
        }

        public UpdateProxyOperation<T> build() {

            return new UpdateProxyOperation<>(
                this.client, this.functionName, new ArrayList<>(arguments.values()), this.argumentsMapper,
                this.resultMapper);
        }
    }
}
