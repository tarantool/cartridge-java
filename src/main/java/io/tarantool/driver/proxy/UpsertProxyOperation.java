package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;

/**
 * Proxy operation for upsert
 *
 * @param <T> tuple result type
 * @author Sergey Volgin
 */
public final class UpsertProxyOperation<T> extends AbstractProxyOperation<T> {

    UpsertProxyOperation(TarantoolClient client,
                         String functionName,
                         List<Object> arguments,
                         TarantoolCallResultMapper<T> resultMapper) {
        super(client, functionName, arguments, resultMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T> {
        private TarantoolClient client;
        private String spaceName;
        private String functionName;
        private TarantoolTuple tuple;
        private TupleOperations operations;
        private TarantoolCallResultMapper<T> resultMapper;

        public Builder() {
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

        public Builder<T> withTuple(TarantoolTuple tuple) {
            this.tuple = tuple;
            return this;
        }

        public Builder<T> withTupleOperation(TupleOperations operations) {
            this.operations = operations;
            return this;
        }

        public Builder<T> withResultMapper(TarantoolCallResultMapper<T> resultMapper) {
            this.resultMapper = resultMapper;
            return this;
        }

        public UpsertProxyOperation<T> build() {
            Assert.notNull(client, "Tarantool client should not be null");
            Assert.notNull(spaceName, "Tarantool spaceName should not be null");
            Assert.notNull(functionName, "Proxy delete function name should not be null");
            Assert.notNull(tuple, "Tarantool tuple should not be null");
            Assert.notNull(operations, "Tarantool tuple operations should not be null");
            Assert.notNull(resultMapper, "Result tuple mapper should not be null");

            TarantoolClientConfig config = client.getConfig();
            CRUDOperationOptions options = CRUDOperationOptions.builder()
                    .withTimeout(config.getRequestTimeout())
                    .build();

            List<Object> arguments = Arrays.asList(spaceName,
                    tuple.getFields(),
                    operations.asProxyOperationList(),
                    options.asMap());

            return new UpsertProxyOperation<T>(this.client, this.functionName, arguments, this.resultMapper);
        }
    }
}
