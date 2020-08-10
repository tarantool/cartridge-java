package io.tarantool.driver.api.tuple;

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
     */
    <O> O getValue(Class<O> targetClass);

    /**
     * Get the field value converted to {@code byte[]}
     * @return value
     */
    byte[] getByteArray();

    /**
     * Get the field value converted to {@code Boolean}
     * @return value
     */
    Boolean getBoolean();

    /**
     * Get the field value converted to {@code Double}
     * @return value
     */
    Double getDouble();

    /**
     * Get the field value converted to {@code Integer}
     * @return value
     */
    Integer getInteger();

    /**
     * Get the field value converted to {@code String}
     * @return value
     */
    String getString();

    /**
     * Get the field value converted to {@link UUID}
     * @return value
     */
    UUID getUUID();

    /**
     * Get the field value converted to {@link BigDecimal}
     * @return value
     */
    BigDecimal getDecimal();
}
