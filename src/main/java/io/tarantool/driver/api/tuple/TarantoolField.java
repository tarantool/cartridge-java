package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.protocol.Packable;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Represents individual field in a tuple
 *
 * @author Alexey Kuzin
 */
public interface TarantoolField extends Packable {
    /**
     * Get the field value converted to the target type
     * @param targetClass the target type class
     * @param <O> the target type
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    <O> O getValue(Class<O> targetClass) throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@code byte[]}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    byte[] getByteArray() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@code Boolean}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    Boolean getBoolean() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@code Double}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    Double getDouble() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@code Integer}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    Integer getInteger() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@code String}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    String getString() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@link UUID}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    UUID getUUID() throws TarantoolValueConverterNotFoundException;

    /**
     * Get the field value converted to {@link BigDecimal}
     * @return value
     * @throws TarantoolValueConverterNotFoundException if the corresponding converter is not found
     */
    BigDecimal getDecimal() throws TarantoolValueConverterNotFoundException;
}
