package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.ReplaceManyOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * Proxy operation for replacing many records at once
 *
 * @param <T> result type
 * @param <R> result collection type
 * @author Alexey Kuzin
 */
public final class ReplaceManyProxyOperation<T extends Packable, R extends Collection<T>>
    extends AbstractProxyOperation<R> {

    ReplaceManyProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<R, SingleValueCallResult<R>>> resultMapperSupplier) {
        super(client, functionName, arguments, argumentsMapperSupplier, resultMapperSupplier);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T extends Packable, R extends Collection<T>>
        extends GenericOperationsBuilder<R, ReplaceManyOptions<?>, Builder<T, R>> implements
        OperationWithTuplesBuilderOptions<Builder<T, R>, T> {

        public Builder() {
        }

        @Override
        public Builder<T, R> self() {
            return this;
        }

        public ReplaceManyProxyOperation<T, R> build() {

            return new ReplaceManyProxyOperation<>(
                this.client, this.functionName, this.arguments.values(),
                this.argumentsMapperSupplier, this.resultMapperSupplier);
        }
    }
}
