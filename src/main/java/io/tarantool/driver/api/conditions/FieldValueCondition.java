package io.tarantool.driver.api.conditions;

/**
 * Tuple filtering condition which accepts a field value
 *
 * @author Alexey Kuzin
 */
public class FieldValueCondition extends BaseCondition {

    private final Object value;

    /**
     * Basic constructor
     *
     * @param operator filtering operator
     * @param field filtering field
     * @param value field value for comparison
     */
    public FieldValueCondition(Operator operator, FieldIdentifier<?, ?> field, Object value) {
        super(operator, field);
        this.value = value;
    }

    @Override
    public Object value() {
        return value;
    }
}
