package io.tarantool.driver.api.conditions;

import java.util.List;

/**
 * Tuple filtering condition which accepts index key parts values
 *
 * @author Alexey Kuzin
 */
public interface IndexValueCondition extends Condition {
    @Override
    List<?> value();
}
