package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.core.metadata.TarantoolSpaceMetadata;

import java.util.Arrays;
import java.util.List;

/**
 * Basic class for tuple filtering conditions
 *
 * @author Alexey Kuzin
 */
public abstract class BaseCondition implements Condition {

    private final Operator operator;
    private final FieldIdentifier<?, ?> field;

    public BaseCondition(Operator operator, FieldIdentifier<?, ?> field) {
        this.operator = operator;
        this.field = field;
    }

    @Override
    public Operator operator() {
        return operator;
    }

    @Override
    public FieldIdentifier<?, ?> field() {
        return field;
    }

    @Override
    public List<?> toList(TarantoolMetadataOperations metadataOperations, TarantoolSpaceMetadata spaceMetadata) {
        return Arrays.asList(operator.getCode(), field.toIdentifier(), value());
    }
}
