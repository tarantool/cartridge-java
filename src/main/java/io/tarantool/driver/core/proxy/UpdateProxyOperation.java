package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.UpdateOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.Collection;
import java.util.function.Supplier;

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
        Collection<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
        super(client, functionName, arguments, argumentsMapper, resultMapperSupplier);
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
                this.client, this.functionName, this.arguments.values(),
                this.argumentsMapper, this.resultMapperSupplier);
        }
    }
}
