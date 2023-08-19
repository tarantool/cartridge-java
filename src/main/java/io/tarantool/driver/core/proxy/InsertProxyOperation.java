package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Proxy operation for insert
 *
 * @param <T> result type
 * @param <R> result collection type
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class InsertProxyOperation<T extends Packable, R extends Collection<T>> extends AbstractProxyOperation<R> {

    private InsertProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        Supplier<CallResultMapper<R, SingleValueCallResult<R>>> resultMapperSupplier) {
        super(client, functionName, arguments, argumentsMapper, resultMapperSupplier);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T extends Packable, R extends Collection<T>>
        extends GenericOperationsBuilder<R, InsertOptions<?>, Builder<T, R>> implements
        OperationWithTupleBuilderOptions<Builder<T, R>, T> {

        public Builder() {
        }

        @Override
        public Builder<T, R> self() {
            return this;
        }

        public InsertProxyOperation<T, R> build() {

            return new InsertProxyOperation<>(
                this.client, this.functionName, this.arguments.values(),
                this.argumentsMapper, this.resultMapperSupplier);
        }
    }
}
