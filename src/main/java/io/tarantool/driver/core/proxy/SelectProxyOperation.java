package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.SingleValueCallResult;
import io.tarantool.driver.api.TarantoolCallOperations;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.options.SelectOptions;
import io.tarantool.driver.mappers.CallResultMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

import java.util.Arrays;
import java.util.List;
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
        extends GenericOperationsBuilder<T, SelectOptions, Builder<T>> {
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
            CRUDSelectOptions.Builder requestOptions = new CRUDSelectOptions.Builder()
                .withTimeout(options.getTimeout())
                .withSelectBatchSize(options.getBatchSize())
                .withSelectLimit(Optional.of(conditions.getLimit()))
                .withSelectAfter(Optional.ofNullable(conditions.getStartTuple()))
                .withBucketId(options.getBucketId());

            List<?> arguments = Arrays.asList(
                spaceName,
                conditions.toProxyQuery(operations, metadata),
                requestOptions.build().asMap()
            );

            return new SelectProxyOperation<>(
                this.client, this.functionName, arguments, this.argumentsMapper, this.resultMapper);
        }
    }
}
