package io.tarantool.driver.api.conditions;

import java.util.List;

/**
 * Tuple filtering condition which accepts index key parts values
 *
 * @author Alexey Kuzin
 */
public class IndexValueCondition extends BaseCondition {

    private final List<Object> indexValues;

    /**
     * Basic constructor
     *
     * @param operator the filtering operator
     * @param field the filtering index
     * @param indexValues the index parts values
     */
    public IndexValueCondition(Operator operator, FieldIdentifier<?, ?> field, List<Object> indexValues) {
        super(operator, field);
        this.indexValues = indexValues;
    }

    @Override
    public List<Object> value() {
        return indexValues;
    }
}
