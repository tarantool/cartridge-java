package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

import java.util.Arrays;
import java.util.List;

/**
 * Proxy operation for select
 *
 * @param <T> result type
 * @author Sergey Volgin
 */
public final class SelectProxyOperation<T> extends AbstractProxyOperation<T> {

    private SelectProxyOperation(TarantoolCallOperations client,
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
        private final TarantoolMetadataOperations operations;
        private final TarantoolSpaceMetadata metadata;
        private TarantoolCallOperations client;
        private String spaceName;
        private String functionName;
        private MessagePackObjectMapper argumentsMapper;
        private CallResultMapper<T, SingleValueCallResult<T>> resultMapper;
        private Conditions conditions;
        private int requestTimeout;

        public Builder(TarantoolMetadataOperations operations, TarantoolSpaceMetadata metadata) {
            this.operations = operations;
            this.metadata = metadata;
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

        public Builder<T> withConditions(Conditions conditions) {
            this.conditions = conditions;
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

        public SelectProxyOperation<T> build() {
            CRUDOperationOptions.Builder requestOptions = CRUDOperationOptions.builder()
                    .withTimeout(requestTimeout)
                    .withSelectBatchSize(conditions.getLimit())
                    .withSelectLimit(conditions.getLimit())
                    .withSelectAfter(conditions.getStartTuple());

            List<?> arguments = Arrays.asList(
                    spaceName,
                    conditions.toProxyQuery(operations, metadata),
                    requestOptions.build().asMap()
            );

            return new SelectProxyOperation<>(
                    this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
