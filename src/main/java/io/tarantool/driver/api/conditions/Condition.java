package io.tarantool.driver.api.conditions;

/**
 * Represents a condition which is evaluated against the tuple fields
 *
 * @author Alexey Kuzin
 */
public interface Condition {
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
    FieldIdentifier field();

    /**
     * Filtering value for the operand
     *
     * @return operand value
     */
    Object value();
}
