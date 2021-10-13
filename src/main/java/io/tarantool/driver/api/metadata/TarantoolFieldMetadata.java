package io.tarantool.driver.api.metadata;

import java.io.Serializable;

/**
 * Tarantool space field format metadata
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public interface TarantoolFieldMetadata extends Serializable {
    /**
     * Get field name
     *
     * @return field name
     */
    String getFieldName();

    /**
     * Get field type
     *
     * @return field type
     */
    String getFieldType();

    /**
     * Get field position in space starts with 0
     *
     * @return field position in space starts with 0
     */
    int getFieldPosition();

    /**
     * Get isNullable parameter
     *
     * @return is_nullable parameter
     */
    boolean getIsNullable();
}
