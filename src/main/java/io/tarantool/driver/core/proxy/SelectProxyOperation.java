package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;
import io.tarantool.driver.api.space.options.SelectOptions;
import io.tarantool.driver.core.proxy.enums.ProxyOperationArgument;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Proxy operation for select
 *
 * @param <T> result type
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class SelectProxyOperation<T> extends AbstractProxyOperation<T> {

    private SelectProxyOperation(
        TarantoolCallOperations client,
        String functionName,
        Collection<?> arguments,
        Supplier<MessagePackObjectMapper> argumentsMapperSupplier,
        Supplier<CallResultMapper<T, SingleValueCallResult<T>>> resultMapperSupplier) {
        super(client, functionName, arguments, argumentsMapperSupplier, resultMapperSupplier);
    }

    /**
     * The builder for this class.
     */
    public static final class Builder<T>
        extends GenericOperationsBuilder<T, SelectOptions<?>, Builder<T>> {
        private final TarantoolMetadataOperations operations;
        private final TarantoolSpaceMetadata metadata;
        private Conditions conditions;

        public Builder(TarantoolMetadataOperations operations, TarantoolSpaceMetadata metadata) {
            this.operations = operations;
            this.metadata = metadata;
        }

        @Override
        public Builder<T> self() {
            return this;
        }

        public Builder<T> withConditions(Conditions conditions) {
            this.conditions = conditions;
            return this;
        }

        public SelectProxyOperation<T> build() {

            addArgument(ProxyOperationArgument.PROXY_QUERY,
                this.conditions.toProxyQuery(this.operations, this.metadata));

            Map<String, Object> options = (Map<String, Object>) arguments.get(ProxyOperationArgument.OPTIONS);

            options.put(ProxyOption.FIRST.toString(), conditions.getLimit());

            Optional.ofNullable(conditions.getStartTuple())
                .ifPresent(after -> options.put(ProxyOption.AFTER.toString(), after));

            return new SelectProxyOperation<>(
                this.client, this.functionName, this.arguments.values(),
                this.argumentsMapperSupplier, this.resultMapperSupplier);
        }
    }
}
