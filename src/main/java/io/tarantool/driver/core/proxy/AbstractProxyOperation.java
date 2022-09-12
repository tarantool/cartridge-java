package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Basic implementation of a proxy operation
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
abstract class AbstractProxyOperation<T> implements ProxyOperation<T> {

    protected final TarantoolCallOperations client;
    protected final String functionName;
    protected final List<?> arguments;
    private final MessagePackObjectMapper argumentsMapper;
    protected final CallResultMapper<T, SingleValueCallResult<T>> resultMapper;

    AbstractProxyOperation(
            TarantoolCallOperations client,
            String functionName,
            List<?> arguments,
            MessagePackObjectMapper argumentsMapper,
            CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        this.client = client;
        this.argumentsMapper = argumentsMapper;
        this.arguments = arguments;
        this.functionName = functionName;
        this.resultMapper = resultMapper;
    }

    public TarantoolCallOperations getClient() {
        return client;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<?> getArguments() {
        return arguments;
    }

    public CallResultMapper<T, SingleValueCallResult<T>> getResultMapper() {
        return resultMapper;
    }

    @Override
    public CompletableFuture<T> execute() {
        return client.callForSingleResult(functionName, arguments, argumentsMapper, resultMapper);
    }

    abstract static class GenericOperationsBuilder<T, B extends GenericOperationsBuilder<T, B>> {
        protected TarantoolCallOperations client;
        protected String spaceName;
        protected String functionName;
        protected MessagePackObjectMapper argumentsMapper;
        protected CallResultMapper<T, SingleValueCallResult<T>> resultMapper;
        protected int requestTimeout;
        protected List<String> fields;

        GenericOperationsBuilder() {
        }

        abstract B self();

        /**
         * Specify a client for sending and receiving requests from Tarantool server
         *
         * @param client Tarantool server client
         * @return builder
         */
        public B withClient(TarantoolCallOperations client) {
            this.client = client;
            return self();
        }

        /**
         * Specify name of Tarantool server space to work with
         *
         * @param spaceName name of Tarantool server space
         * @return builder
         */
        public B withSpaceName(String spaceName) {
            this.spaceName = spaceName;
            return self();
        }

        /**
         * Specify name of the Tarantool server function called through preparing request
         *
         * @param functionName name of Tarantool server function
         * @return builder
         */
        public B withFunctionName(String functionName) {
            this.functionName = functionName;
            return self();
        }

        public B withArgumentsMapper(MessagePackObjectMapper objectMapper) {
            this.argumentsMapper = objectMapper;
            return self();
        }

        public B withResultMapper(CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
            this.resultMapper = resultMapper;
            return self();
        }

        /**
         * Specify response reading timeout
         *
         * @param requestTimeout the timeout for reading the responses from Tarantool server, in milliseconds
         * @return builder
         */
        public B withRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return self();
        }

        /**
         * Specify response fields
         *
         * @param fields {@link List} of names for getting only a subset of fields
         * @return builder
         */
        public B withFields(List<String> fields) {
            this.fields = fields;
            return self();
        }
    }
}
