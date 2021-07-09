package io.tarantool.driver.api.conditions;

import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a condition which is evaluated against the tuple fields
 *
 * @author Alexey Kuzin
 */
public interface Condition extends Serializable {
    /**
     * Filtering operator (&lt;. &lt;=, =, =&gt;, &gt;)
     *
     * @return operator
     */
    Operator operator();

    /**
     * Filtering operand, may be a field or index
     *
     * @return operand
     */
    FieldIdentifier<?, ?> field();

    /**
     * Filtering value for the operand
     *
     * @return operand value
     */
    Object value();

    /**
     * Serializes the condition into a form of Java list
     *
     * @param metadataOperations metadata operations
     * @param spaceMetadata space metadata
     * @return list of serialized conditions
     */
    List<?> toList(TarantoolMetadataOperations metadataOperations, TarantoolSpaceMetadata spaceMetadata);
}
