package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.SelectOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.protocol.Packable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
        List<?> arguments,
        MessagePackObjectMapper argumentsMapper,
        CallResultMapper<T, SingleValueCallResult<T>> resultMapper) {
        super(client, functionName, arguments, argumentsMapper, resultMapper);
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
        Builder<T> self() {
            return this;
        }

        public Builder<T> withConditions(Conditions conditions) {
            this.conditions = conditions;
            return this;
        }

        public SelectProxyOperation<T> build() {

            Optional<Long> first = options.getFirst();
            if (first.isPresent()) {
                options.addOption(ProxyOption.FIRST, Math.min(conditions.getLimit(), first.get()));
            } else {
                options.addOption(ProxyOption.FIRST, conditions.getLimit());
            }

            Packable tuple = conditions.getStartTuple();
            if (Objects.nonNull(tuple)) {
                options.addOption(ProxyOption.AFTER, conditions.getStartTuple());
            }

            List<?> arguments = Arrays.asList(
                spaceName,
                conditions.toProxyQuery(operations, metadata),
                options.asMap()
            );

            return new SelectProxyOperation<>(
                this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
