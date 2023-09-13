package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.interfaces.UpdateOptions;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolIndexQuery;

import java.util.Arrays;
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
        extends GenericOperationsBuilder<T, UpdateOptions, Builder<T>> {
        private TarantoolIndexQuery indexQuery;
        private TupleOperations operations;

        public Builder() {
        }

        @Override
        Builder<T> self() {
            return this;
        }

        public Builder<T> withIndexQuery(TarantoolIndexQuery indexQuery) {
            this.indexQuery = indexQuery;
            return this;
        }

        public Builder<T> withTupleOperation(TupleOperations operations) {
            this.operations = operations;
            return this;
        }

        public UpdateProxyOperation<T> build() {
            CRUDBucketIdOptions requestOptions = new CRUDUpdateOptions.Builder()
                .withTimeout(options.getTimeout())
                .withBucketId(options.getBucketId())
                .withFields(options.getFields())
                .build();

            List<?> arguments = Arrays.asList(spaceName,
                indexQuery.getKeyValues(),
                operations.asProxyOperationList(),
                requestOptions.asMap());

            return new UpdateProxyOperation<>(
                this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
