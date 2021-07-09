package io.tarantool.driver.api.conditions;

import java.util.Objects;

/**
 * Tuple filtering condition which accepts a field value
 *
 * @author Alexey Kuzin
 */
public class FieldValueCondition extends BaseCondition {

    private static final long serialVersionUID = 20200708L;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FieldValueCondition that = (FieldValueCondition) o;
        return Objects.equals(value, that.value) &&
                operator() == that.operator() &&
                Objects.equals(field(), that.field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, operator(), field());
    }
}
