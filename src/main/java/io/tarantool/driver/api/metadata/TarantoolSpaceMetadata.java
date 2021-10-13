package io.tarantool.driver.api.metadata;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public interface TarantoolSpaceMetadata extends Serializable {
    /**
     * Get space ID on the Tarantool server
     *
     * @return a number
     */
    int getSpaceId();

    /**
     * Get owner ID
     *
     * @return a number
     */
    int getOwnerId();

    /**
     * Get space name
     *
     * @return a non-empty {@code String}
     */
    String getSpaceName();

    /**
     * Get map with metadata of fields
     *
     * @return map whose key is field name and value is {@link TarantoolFieldMetadata}
     */
    Map<String, TarantoolFieldMetadata> getSpaceFormatMetadata();

    /**
     * Get field metadata by name
     *
     * @param fieldName field name
     * @return field position by name starting with 0, or -1 if this field not found in format metadata
     */
    Optional<TarantoolFieldMetadata> getFieldByName(String fieldName);

    /**
     * Get field metadata by position
     *
     * @param fieldPosition field position starting with 0
     * @return field name or null if this field not found in format metadata
     */
    Optional<TarantoolFieldMetadata> getFieldByPosition(int fieldPosition);

    /**
     * Get field position in space by name starts with 0, or -1 if this field not found in format metadata
     *
     * @param fieldName field name
     * @return field position by name starting with 0, or -1 if this field not found in format metadata
     */
    int getFieldPositionByName(String fieldName);

    /**
     * Get field name by position
     *
     * @param fieldPosition field position starting with 0
     * @return field name or null if this field not found in format metadata
     */
    Optional<String> getFieldNameByPosition(int fieldPosition);
}
