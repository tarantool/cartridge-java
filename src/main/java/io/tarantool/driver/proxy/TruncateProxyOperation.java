package io.tarantool.driver.proxy;

import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolVoidResult;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Proxy operation for truncate
 *
 * @author Ivan Dneprov
 */
public final class TruncateProxyOperation implements ProxyOperation<Void> {

    private final TarantoolCallOperations client;
    private final String functionName;
    private final List<?> arguments;

    private TruncateProxyOperation(TarantoolCallOperations client,
                                   String functionName,
                                   List<?> arguments) {
        this.client = client;
        this.arguments = arguments;
        this.functionName = functionName;
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

    @Override
    public CompletableFuture<Void> execute() {
        return client.callForSingleResult(functionName, arguments, Boolean.class)
                .thenApply(v -> TarantoolVoidResult.INSTANCE.value());
    }

    /**
     * Create a builder instance.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private TarantoolCallOperations client;
        private String spaceName;
        private String functionName;
        private int requestTimeout;

        public Builder() {
        }

        /**
         * Specify a client for sending and receiving requests from Tarantool server
         * @param client Tarantool server client
         * @return builder
         */
        public Builder withClient(TarantoolCallOperations client) {
            this.client = client;
            return this;
        }

        /**
         * Specify name of Tarantool server space to work with
         * @param spaceName name of Tarantool server space
         * @return builder
         */
        public Builder withSpaceName(String spaceName) {
            this.spaceName = spaceName;
            return this;
        }

        /**
         * Specify name of the Tarantool server function called through preparing request
         * @param functionName name of Tarantool server function
         * @return builder
         */
        public Builder withFunctionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        /**
         * Specify response reading timeout.
         * @param requestTimeout the timeout for reading the responses from Tarantool server, in milliseconds
         * @return builder
         */
        public Builder withRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Prepare request of truncate operation to Tarantool server
         * @return TruncateProxyOperation instance
         */
        public TruncateProxyOperation build() {
            CRUDOperationOptions options = CRUDOperationOptions.builder().withTimeout(requestTimeout).build();

            List<?> arguments = Arrays.asList(spaceName, options.asMap());

            return new TruncateProxyOperation(this.client, this.functionName, arguments);
        }
    }
}
