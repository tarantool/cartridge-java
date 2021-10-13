package io.tarantool.driver.core.conditions;

import io.tarantool.driver.api.conditions.FieldIdentifier;
import io.tarantool.driver.api.conditions.IndexValueCondition;
import io.tarantool.driver.api.conditions.Operator;

import java.util.List;
import java.util.Objects;

/**
 * Tuple filtering condition which accepts index key parts values
 *
 * @author Alexey Kuzin
 */
public class IndexValueConditionImpl extends BaseCondition implements IndexValueCondition {

    private static final long serialVersionUID = 20200708L;

    private final List<?> indexValues;

    /**
     * Basic constructor
     *
     * @param operator the filtering operator
     * @param field the filtering index
     * @param indexValues the index parts values
     */
    public IndexValueConditionImpl(Operator operator, FieldIdentifier<?, ?> field, List<?> indexValues) {
        super(operator, field);
        this.indexValues = indexValues;
    }

    @Override
    public List<?> value() {
        return indexValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexValueConditionImpl that = (IndexValueConditionImpl) o;
        return indexValues.equals(that.indexValues) &&
                operator() == that.operator() &&
                field().equals(that.field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexValues, operator(), field());
    }
}
