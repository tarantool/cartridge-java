package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.InsertManyOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Arrays;
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

    InsertManyProxyOperation(TarantoolCallOperations client,
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
        extends GenericOperationsBuilder<R, InsertManyOptions, Builder<T, R>> {
        private Collection<T> tuples;

        public Builder() {
        }

        @Override
        Builder<T, R> self() {
            return this;
        }

        public Builder<T, R> withTuples(Collection<T> tuples) {
            this.tuples = tuples;
            return this;
        }

        public InsertManyProxyOperation<T, R> build() {
            if (tuples == null) {
                throw new IllegalArgumentException("Tuples must be specified for batch insert operation");
            }

            CRUDBatchOptions requestOptions = new CRUDBatchOptions.Builder()
                    .withTimeout(options.getTimeout())
                    .withStopOnError(options.getStopOnError())
                    .withRollbackOnError(options.getRollbackOnError())
                    .build();

            List<?> arguments = Arrays.asList(spaceName, tuples, requestOptions.asMap());

            return new InsertManyProxyOperation<>(
                    this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
