package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.interfaces.InsertOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
        List<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        CallResultMapper<R, SingleValueCallResult<R>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T extends Packable, R extends Collection<T>>
        extends GenericOperationsBuilder<R, InsertOptions<?>, Builder<T, R>> {
        private T tuple;

        public Builder() {
        }

        @Override
        Builder<T, R> self() {
            return this;
        }

        public Builder<T, R> withTuple(T tuple) {
            this.tuple = tuple;
            return this;
        }

        public InsertProxyOperation<T, R> build() {

            List<?> arguments = Arrays.asList(spaceName, tuple, options.asMap());

            return new InsertProxyOperation<>(
                this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
