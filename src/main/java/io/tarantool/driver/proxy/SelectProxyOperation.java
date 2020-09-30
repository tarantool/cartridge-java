package io.tarantool.driver.proxy;

import io.tarantool.driver.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.mappers.ValueConverter;
import io.tarantool.driver.protocol.operations.TupleOperation;
import io.tarantool.driver.protocol.operations.TupleOperationAdd;
import io.tarantool.driver.protocol.operations.TupleOperationSet;
import org.msgpack.value.ArrayValue;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class SelectProxyOperation<T> extends AbstractProxyOperation<T> {

    private SelectProxyOperation(TarantoolClient client,
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
        private TarantoolIndexQuery indexQuery;
        private ValueConverter<ArrayValue, T> tupleMapper;
        private TarantoolSelectOptions options;

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

        public Builder<T> withIndexQuery(TarantoolIndexQuery indexQuery) {
            this.indexQuery = indexQuery;
            return this;
        }

        public Builder<T> withSelectOptions(TarantoolSelectOptions options) {
            this.options = options;
            return this;
        }

        public Builder<T> withValueConverter(ValueConverter<ArrayValue, T> tupleMapper) {
            this.tupleMapper = tupleMapper;
            return this;
        }

        public SelectProxyOperation<T> build() {
            Assert.notNull(client, "Tarantool client should not be null");
            Assert.notNull(spaceName, "Tarantool spaceName should not be null");
            Assert.notNull(functionName, "Proxy delete function name should not be null");
            Assert.notNull(indexQuery, "Tarantool indexQuery should not be null");
            Assert.notNull(tupleMapper, "Tuple mapper should not be null");

            TarantoolClientConfig config = client.getConfig();

            CRUDOperationOptions requestOptions = CRUDOperationOptions.builder()
                    .withTimeout(config.getRequestTimeout())
                    .withTuplesToMap(false)
                    .withSelectBatchSize(options.getLimit())
                    .withSelectLimit(options.getLimit())
                    .build();

            //options.getOffset() getIterator() TODO: warning / exception
            //indexQuery.getKeyValues()  TODO:

//            List<TupleOperation> operations = new ArrayList<>();
//
//            switch (indexQuery.getIteratorType()) {
//                case ITER_EQ: operations.add(new TupleOperationSet(fieldIndex, value));
//            }

            List<Object> arguments = Arrays.asList(spaceName, Collections.emptyList(), requestOptions.asMap());

            return new SelectProxyOperation<T>(this.client, this.functionName, arguments, this.tupleMapper);
        }
    }
}
