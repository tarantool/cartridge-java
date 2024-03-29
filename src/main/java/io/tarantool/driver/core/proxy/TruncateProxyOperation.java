package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.TarantoolVoidResult;
import io.tarantool.driver.api.space.options.crud.OperationWithTimeoutOptions;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Proxy operation for truncate
 *
 * @author Ivan Dneprov
 * @author Artyom Dubinin
 */
public final class TruncateProxyOperation implements ProxyOperation<Void> {

    private final TarantoolCallOperations client;
    private final String functionName;
    private final Collection<?> arguments;

    private TruncateProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments) {
        this.client = client;
        this.arguments = arguments;
        this.functionName = functionName;
    }

    /**
     * Create a builder instance.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    public TarantoolCallOperations getClient() {
        return client;
    }

    public String getFunctionName() {
        return functionName;
    }

    public Collection<?> getArguments() {
        return arguments;
    }

    @Override
    public CompletableFuture<Void> execute() {
        return client.callForSingleResult(functionName, arguments, Boolean.class)
            .thenApply(v -> TarantoolVoidResult.INSTANCE.value());
    }

    public static final class Builder
        extends AbstractProxyOperation.GenericOperationsBuilder<Void, OperationWithTimeoutOptions<?>, Builder> {

        public Builder() {
        }

        @Override
        public Builder self() {
            return this;
        }

        /**
         * Prepare request of truncate operation to Tarantool server
         *
         * @return TruncateProxyOperation instance
         */
        public TruncateProxyOperation build() {

            return new TruncateProxyOperation(this.client, this.functionName, this.arguments.values());
        }
    }
}
