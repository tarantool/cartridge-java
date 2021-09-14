package io.tarantool.driver.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
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
    public static final class Builder<T extends Packable, R extends Collection<T>> {
        private TarantoolCallOperations client;
        private String spaceName;
        private String functionName;
        private T tuple;
        private TupleOperations operations;
        private MessagePackObjectMapper argumentsMapper;
        private CallResultMapper<R, SingleValueCallResult<R>> resultMapper;
        private int requestTimeout;

        public Builder() {
        }

        public Builder<T, R> withClient(TarantoolCallOperations client) {
            this.client = client;
            return this;
        }

        public Builder<T, R> withSpaceName(String spaceName) {
            this.spaceName = spaceName;
            return this;
        }

        public Builder<T, R> withFunctionName(String functionName) {
            this.functionName = functionName;
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

        public Builder<T, R> withArgumentsMapper(MessagePackObjectMapper objectMapper) {
            this.argumentsMapper = objectMapper;
            return this;
        }

        public Builder<T, R> withResultMapper(CallResultMapper<R, SingleValueCallResult<R>> resultMapper) {
            this.resultMapper = resultMapper;
            return this;
        }

        public Builder<T, R> withRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public UpsertProxyOperation<T, R> build() {
            CRUDOperationOptions options = CRUDOperationOptions.builder().withTimeout(requestTimeout).build();

            List<?> arguments = Arrays.asList(spaceName, tuple, operations.asProxyOperationList(), options.asMap());

            return new UpsertProxyOperation<>(
                    this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
