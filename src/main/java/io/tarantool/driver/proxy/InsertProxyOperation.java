package io.tarantool.driver.proxy;


import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.List;

/**
 *
 * @param <T>
 */
public final class InsertProxyOperation<T> extends AbstractProxyOperation<T> {

    private InsertProxyOperation(TarantoolClient client,
                                String functionName,
                                List<Object> arguments,
                                ValueConverter<ArrayValue, T> tupleMapper) {
        super(client, functionName, arguments, tupleMapper);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T> {
        private TarantoolClient client;
        private String spaceName;
        private String functionName;
        private TarantoolTuple tuple;
        private ValueConverter<ArrayValue, T> tupleMapper;

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

        public Builder<T> withValueConverter(ValueConverter<ArrayValue, T> tupleMapper) {
            this.tupleMapper = tupleMapper;
            return this;
        }

        public InsertProxyOperation<T> build() {
            Assert.notNull(client, "Tarantool client should not be null");
            Assert.notNull(spaceName, "Tarantool spaceName should not be null");
            Assert.notNull(functionName, "Proxy delete function name should not be null");
            Assert.notNull(tuple, "Tarantool tuple should not be null");
            Assert.notNull(tupleMapper, "Tuple mapper should not be null");

            TarantoolClientConfig config = client.getConfig();
            CRUDOperationOptions options = CRUDOperationOptions.builder()
                    .withTimeout(config.getRequestTimeout())
                    .withTuplesToMap(false)
                    .build();

            List<Object> arguments = Arrays.asList(spaceName, tuple.getFields(), options.asMap());

            return new InsertProxyOperation<T>(this.client, this.functionName, arguments, this.tupleMapper);
        }
    }
}