package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.mappers.TarantoolCallResultMapper;
import org.springframework.util.Assert;

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
        private TarantoolClient client;
        private String spaceName;
        private String functionName;
        private List<?> selectArguments;
        private TarantoolCallResultMapper<T> resultMapper;
        private TarantoolSelectOptions selectOptions;

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

        public Builder<T> withSelectArguments(List<?> selectArguments) {
            this.selectArguments = selectArguments;
            return this;
        }

        public Builder<T> withSelectOptions(TarantoolSelectOptions selectOptions) {
            this.selectOptions = selectOptions;
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
            Assert.notNull(selectOptions, "Tarantool select options should not be null");

            TarantoolClientConfig config = client.getConfig();

            CRUDOperationOptions.Builder requestOptions = CRUDOperationOptions.builder()
                    .withTimeout(config.getRequestTimeout())
                    .withSelectBatchSize(selectOptions.getLimit())
                    .withSelectLimit(selectOptions.getLimit());

            List<Object> arguments = Arrays.asList(spaceName, selectArguments, requestOptions.build().asMap());

            return new SelectProxyOperation<T>(this.client, this.functionName, arguments, this.resultMapper);
        }
    }
}
