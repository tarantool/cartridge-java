package io.tarantool.driver.api.tuple;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.tarantool.driver.mappers.converters.Interval;
import io.tarantool.driver.protocol.Packable;

/**
 * Basic Tarantool atom of data
 *
 * @author Alexey Kuzin
 */
public interface TarantoolTuple extends Iterable<TarantoolField>, Packable {
    /**
     * Check whether this tuple includes information about space format
     *
     * @return true if we can obtain the tuple fields by names
     */
    boolean hasMetadata();

    /**
     * Get a tuple field by its position
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
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
     *
     * @param fieldPosition field position from the tuple start, starting from 0
     * @param objectClass   target value type class
     * @param <O>           target value type
     * @return nullable value of a field wrapped in Optional, possibly converted to a Java type
     */
    <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass);

    /**
     * Check if a tuple field exists and can be converted to the target value type
     *
     * @param fieldPosition field position from the tuple start, starting from 0
     * @param objectClass   target value type class
     * @return true, if the field exists and can be converted to the given type, false otherwise
     */
    boolean canGetObject(int fieldPosition, Class<?> objectClass);

    /**
     * Get a tuple field value by its name specifying the target value type
     *
     * @param fieldName   field name, should not be null
     * @param objectClass target value type class
     * @param <O>         target value type
     * @return nullable value of a field wrapped in Optional, possibly converted to a Java type
     */
    <O> Optional<O> getObject(String fieldName, Class<O> objectClass);

    /**
     * Check if a tuple field exists and can be converted to the target value type
     *
     * @param fieldName   field name, should not be null
     * @param objectClass target value type class
     * @return true, if the field exists and can be converted to the given type, false otherwise
     */
    boolean canGetObject(String fieldName, Class<?> objectClass);

    /**
     * Get a tuple field value as a raw object
     *
     * @param fieldPosition field position from the tuple start, starting from 0
     * @return nullable value of a field wrapped in Optional
     */
    Optional<?> getObject(int fieldPosition);

    /**
     * Get a tuple field value as a raw object
     *
     * @param fieldName field name, should not be null
     * @return nullable value of a field wrapped in Optional
     */
    Optional<?> getObject(String fieldName);

    /**
     * Get the number of fields in this tuple
     *
     * @return the number of fields in this tuple
     */
    int size();

    /**
     * Set a tuple field by field position
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @param field         new field
     */
    void setField(int fieldPosition, TarantoolField field);

    /**
     * Set a tuple field by field name
     *
     * @param fieldName the field name, must be not null
     * @param field     new field
     */
    void setField(String fieldName, TarantoolField field);

    /**
     * Set a tuple field value from an object by field position
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @param value         new field value
     */
    void putObject(int fieldPosition, Object value);

    /**
     * Set a tuple field value from an object by field name
     *
     * @param fieldName the field name, must not be null
     * @param value     new field value
     */
    void putObject(String fieldName, Object value);

    /**
     * Get the field value converted to {@code byte[]}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    byte[] getByteArray(int fieldPosition);

    /**
     * Get the field value converted to {@code byte[]}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    byte[] getByteArray(String fieldName);

    /**
     * Get the field value converted to {@code Boolean}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Boolean getBoolean(int fieldPosition);

    /**
     * Get the field value converted to {@code Boolean}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Boolean getBoolean(String fieldName);

    /**
     * Get the field value converted to {@code Double}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Double getDouble(int fieldPosition);

    /**
     * Get the field value converted to {@code Double}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Double getDouble(String fieldName);

    /**
     * Get the field value converted to {@code Float}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Float getFloat(int fieldPosition);

    /**
     * Get the field value converted to {@code Float}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Float getFloat(String fieldName);

    /**
     * Get the field value converted to {@code Integer}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Integer getInteger(int fieldPosition);

    /**
     * Get the field value converted to {@code Integer}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Integer getInteger(String fieldName);

    /**
     * Get the field value converted to {@code Long}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Long getLong(int fieldPosition);

    /**
     * Get the field value converted to {@code Long}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Long getLong(String fieldName);

    /**
     * Get the field value converted to {@code String}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    String getString(int fieldPosition);

    /**
     * Get the field value converted to {@code String}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    String getString(String fieldName);

    /**
     * Get the field value converted to {@code Character}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Character getCharacter(int fieldPosition);

    /**
     * Get the field value converted to {@code Character}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Character getCharacter(String fieldName);

    /**
     * Get the field value converted to {@link UUID}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    UUID getUUID(int fieldPosition);

    /**
     * Get the field value converted to {@link UUID}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    UUID getUUID(String fieldName);

    /**
     * Get the field value converted to {@link Instant}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Instant getInstant(int fieldPosition);

    /**
     * Get the field value converted to {@link Instant}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Instant getInstant(String fieldName);

    /**
     * Get the field value converted to {@link Interval}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Interval getInterval(int fieldPosition);

    /**
     * Get the field value converted to {@link Interval}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Interval getInterval(String fieldName);

    /**
     * Get the field value converted to {@link BigDecimal}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    BigDecimal getDecimal(int fieldPosition);

    /**
     * Get the field value converted to {@link BigDecimal}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    BigDecimal getDecimal(String fieldName);

    /**
     * Get the field value converted to {@link List}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    List<?> getList(int fieldPosition);

    /**
     * Get the field value converted to {@link List}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    List<?> getList(String fieldName);

    /**
     * Get the field value converted to {@link Map}
     *
     * @param fieldPosition the field position from the tuple start, starting from 0
     * @return value
     */
    Map<?, ?> getMap(int fieldPosition);

    /**
     * Get the field value converted to {@link Map}
     *
     * @param fieldName the field name, must not be null
     * @return value
     */
    Map<?, ?> getMap(String fieldName);
}
