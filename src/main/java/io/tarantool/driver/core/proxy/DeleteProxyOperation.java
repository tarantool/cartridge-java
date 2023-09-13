package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.space.options.interfaces.DeleteOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.TarantoolIndexQuery;

import java.util.Arrays;
import java.util.List;

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
        List<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T>
        extends GenericOperationsBuilder<T, DeleteOptions, Builder<T>> {
        private TarantoolIndexQuery indexQuery;

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

        public DeleteProxyOperation<T> build() {
            CRUDBucketIdOptions requestOptions = new CRUDDeleteOptions.Builder()
                .withTimeout(options.getTimeout())
                .withBucketId(options.getBucketId())
                .withFields(options.getFields())
                .build();

            List<?> arguments = Arrays.asList(spaceName, indexQuery.getKeyValues(), requestOptions.asMap());

            return new DeleteProxyOperation<>(
                this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
