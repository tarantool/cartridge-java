package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
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
 */
public final class DeleteProxyOperation<T> extends AbstractProxyOperation<T> {

    private DeleteProxyOperation(TarantoolCallOperations client,
                                 String functionName,
                                 List<?> arguments,
                                 MessagePackObjectMapper argumentsMapper,
                                 CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T> {
        private TarantoolCallOperations client;
        private String spaceName;
        private String functionName;
        private TarantoolIndexQuery indexQuery;
        private MessagePackObjectMapper argumentsMapper;
        private CallResultMapper<T, SingleValueCallResult<T>> resultMapper;
        private int requestTimeout;

        public Builder() {
        }

        public Builder<T> withClient(TarantoolCallOperations client) {
            this.client = client;
            return this;
        }

        public Builder<T> withSpaceName(String spaceName) {
            this.spaceName = spaceName;
            return this;
        }

        public Builder<T> withFunctionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        public Builder<T> withIndexQuery(TarantoolIndexQuery indexQuery) {
            this.indexQuery = indexQuery;
            return this;
        }

        public Builder<T> withArgumentsMapper(MessagePackObjectMapper argumentsMapper) {
            this.argumentsMapper = argumentsMapper;
            return this;
        }

        public Builder<T> withResultMapper(CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
            this.resultMapper = resultMapper;
            return this;
        }

        public Builder<T> withRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        public DeleteProxyOperation<T> build() {
            CRUDOperationOptions options = CRUDOperationOptions.builder().withTimeout(requestTimeout).build();

            List<?> arguments = Arrays.asList(spaceName, indexQuery.getKeyValues(), options.asMap());

            return new DeleteProxyOperation<>(
                    this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
