package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * Proxy operation for select
 *
 * @param <T> tuple result type
 * @author Sergey Volgin
 */
public final class SelectProxyOperation<T> extends AbstractProxyOperation<T> {

    private SelectProxyOperation(TarantoolClient client,
                                 String functionName,
                                 List<Object> arguments,
                                 TarantoolCallResultMapper<T> resultMapper) {
        super(client, functionName, arguments, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T> {
        private final TarantoolMetadataOperations operations;
        private final TarantoolSpaceMetadata metadata;
        private TarantoolClient client;
        private String spaceName;
        private String functionName;
        private TarantoolCallResultMapper<T> resultMapper;
        private Conditions conditions;

        public Builder(TarantoolMetadataOperations operations, TarantoolSpaceMetadata metadata) {
            this.operations = operations;
            this.metadata = metadata;
        }

        public Builder<T> withClient(TarantoolClient client) {
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

        public Builder<T> withResultMapper(TarantoolCallResultMapper<T> resultMapper) {
            this.resultMapper = resultMapper;
            return this;
        }

        public SelectProxyOperation<T> build() {
            Assert.notNull(client, "Tarantool client should not be null");
            Assert.notNull(spaceName, "Tarantool spaceName should not be null");
            Assert.notNull(functionName, "Proxy delete function name should not be null");
            Assert.notNull(resultMapper, "Result tuple mapper should not be null");
            Assert.notNull(conditions, "Select conditions should not be null");

            TarantoolClientConfig config = client.getConfig();

            CRUDOperationOptions.Builder requestOptions = CRUDOperationOptions.builder()
                    .withTimeout(config.getRequestTimeout())
                    .withSelectBatchSize(conditions.getLimit())
                    .withSelectLimit(conditions.getLimit())
                    .withSelectAfter(conditions.getStartTuple());

            List<Object> arguments = Arrays.asList(
                    spaceName,
                    conditions.toProxyQuery(operations, metadata),
                    requestOptions.build().asMap()
            );

            return new SelectProxyOperation<T>(this.client, this.functionName, arguments, this.resultMapper);
        }
    }
}
