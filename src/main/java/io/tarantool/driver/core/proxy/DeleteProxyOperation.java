package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.DeleteOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Proxy operation for delete
 *
 * @param <T> result type
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class DeleteProxyOperation<T> extends AbstractProxyOperation<T> {

    private DeleteProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
        super(client, functionName, arguments, argumentsMapperSupplier, resultMapperSupplier);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T>
        extends GenericOperationsBuilder<T, DeleteOptions<?>, Builder<T>> implements
        OperationWithIndexQueryBuilderOptions<Builder<T>> {

        public Builder() {
        }

        @Override
        public Builder<T> self() {
            return this;
        }

        public DeleteProxyOperation<T> build() {

            return new DeleteProxyOperation<>(
                this.client, this.functionName, this.arguments.values(),
                this.argumentsMapperSupplier, this.resultMapperSupplier);
        }
    }
}
