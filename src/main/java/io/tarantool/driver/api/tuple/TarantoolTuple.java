package io.tarantool.driver.api.tuple;

import io.tarantool.driver.protocol.Packable;

import java.util.List;
import java.util.Optional;

/**
 * Basic Tarantool atom of data
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTuple extends Iterable<TarantoolField>, Packable {
    /**
     * Get a tuple field by its position
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @return field or empty optional if the field position is out of tuple length
     */
    Optional<TarantoolField> getField(int fieldPosition);

    /**
     * Get a tuple field by its name
     *
     * @param fieldName the field name in space
     * @return field or empty optional if the field not exist in space
     */
    Optional<TarantoolField> getField(String fieldName);

    /**
     * Get all tuple fields as list
     *
     * @return all tuple fields as list
     */
    List<TarantoolField> getFields();

    /**
     * Get a tuple field value by its position specifying the target value type
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param objectClass the target value type class
     * @param <O> the target value type
     * @return nullable value of a field wrapped in optional
     */
    <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass);

    /**
     * Get a tuple field value by its name specifying the target value type
     * @param fieldName the field name, should not be null
     * @param objectClass the target value type class
     * @param <O> the target value type
     * @return nullable value of a field wrapped in optional
     */
    <O> Optional<O> getObject(String fieldName, Class<O> objectClass);

    /**
     * Get the number of fields in this tuple
     *
     * @return the number of fields in this tuple
     */
    int size();

    /**
     * Set a tuple field by field position
     *
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param field new field
     */
    void setField(int fieldPosition, TarantoolField field);

    /**
     * Set a tuple field by field name
     *
     * @param fieldName the field name, must be not null
     * @param field new field
     */
    void setField(String fieldName, TarantoolField field);

    /**
     * Set a tuple field value from an object by field position
     *
     * @param fieldPosition the field position from the the tuple start, starting from 0
     * @param value new field value
     */
     void putObject(int fieldPosition, Object value);

    /**
     * Set a tuple field value from an object by field name
     *
     * @param fieldName the field name
     * @param value new field value
     */
     void putObject(String fieldName, Object value);
}
