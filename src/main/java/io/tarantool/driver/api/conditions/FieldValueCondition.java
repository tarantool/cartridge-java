package io.tarantool.driver.api.conditions;

/**
 * Tuple filtering condition which accepts a field value
 *
 * @author Alexey Kuzin
 */
public interface FieldValueCondition extends Condition {
    @Override
    Object value();
}
