package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.UpsertOptions;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Arrays;
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

    UpsertProxyOperation(TarantoolCallOperations client,
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
            extends GenericOperationsBuilder<R, UpsertOptions, Builder<T, R>> {
        private T tuple;
        private TupleOperations operations;

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

        public Builder<T, R> withTupleOperation(TupleOperations operations) {
            this.operations = operations;
            return this;
        }

        public UpsertProxyOperation<T, R> build() {
            CRUDBucketIdOptions requestOptions = new CRUDBucketIdOptions.Builder()
                    .withTimeout(options.getTimeout())
                    .withBucketId(options.getBucketId())
                    .build();

            List<?> arguments = Arrays.asList(
                    spaceName,
                    tuple,
                    operations.asProxyOperationList(),
                    requestOptions.asMap()
            );

            return new UpsertProxyOperation<>(
                    this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
