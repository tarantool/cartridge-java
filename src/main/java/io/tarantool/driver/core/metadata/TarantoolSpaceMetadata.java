package io.tarantool.driver.core.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents Tarantool space metadata (space ID, space name, etc.)
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceMetadata implements Serializable {

    private static final long serialVersionUID = 20200708L;

    private int spaceId;
    private int ownerId;
    private String spaceName;
    private Map<String, TarantoolFieldMetadata> spaceFormatMetadata;
    private List<TarantoolFieldMetadata> spaceFormatMetadataAsList;
    //TODO private TarantoolEngine engine;

    /**
     * Basic constructor.
     */
    public TarantoolSpaceMetadata() {
    }

    /**
     * Get space ID on the Tarantool server
     * @return a number
     */
    public int getSpaceId() {
        return spaceId;
    }

    void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    /**
     * Get owner ID
     * @return a number
     */
    public int getOwnerId() {
        return ownerId;
    }

    void setOwnerId(int ownerId) {
        this.ownerId = ownerId;
    }

    /**
     * Get space name
     * @return a non-empty {@code String}
     */
    public String getSpaceName() {
        return spaceName;
    }

    void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    public Map<String, TarantoolFieldMetadata> getSpaceFormatMetadata() {
        return spaceFormatMetadata;
    }

    void setSpaceFormatMetadata(Map<String, TarantoolFieldMetadata> spaceFormatMetadata) {
        this.spaceFormatMetadata = spaceFormatMetadata;
        this.spaceFormatMetadataAsList = new ArrayList<>(spaceFormatMetadata.values());
    }

    /**
     * Get field metadata by name
     *
     * @param fieldName field name
     * @return field position by name starting with 0, or -1 if this field not found in format metadata
     */
    public Optional<TarantoolFieldMetadata> getFieldByName(String fieldName) {
        TarantoolFieldMetadata fieldMetadata = spaceFormatMetadata.get(fieldName);
        return Optional.ofNullable(fieldMetadata);
    }

    /**
     * Get field metadata by position
     *
     * @param fieldPosition field position starting with 0
     * @return field name or null if this field not found in format metadata
     */
    public Optional<TarantoolFieldMetadata> getFieldByPosition(int fieldPosition) {
        if (fieldPosition >= spaceFormatMetadataAsList.size() || fieldPosition < 0) {
            return Optional.empty();
        }
        TarantoolFieldMetadata fieldMetadata = spaceFormatMetadataAsList.get(fieldPosition);
        return Optional.of(fieldMetadata);
    }

    /**
     * Get field position in space by name starts with 0, or -1 if this field not found in format metadata
     *
     * @param fieldName field name
     * @return field position by name starting with 0, or -1 if this field not found in format metadata
     */
    public int getFieldPositionByName(String fieldName) {
        return getFieldByName(fieldName).map(TarantoolFieldMetadata::getFieldPosition).orElse(-1);
    }

    /**
     * Get field name by position
     *
     * @param fieldPosition field position starting with 0
     * @return field name or null if this field not found in format metadata
     */
    public Optional<String> getFieldNameByPosition(int fieldPosition) {
        return getFieldByPosition(fieldPosition).map(TarantoolFieldMetadata::getFieldName);
    }

    @Override
    public String toString() {
        return "TarantoolSpaceMetadata{" +
                "spaceId=" + spaceId +
                ", ownerId=" + ownerId +
                ", spaceName='" + spaceName + '\'' +
                ", spaceFormatMetadata=" + spaceFormatMetadata +
                ", spaceFormatMetadataAsList=" + spaceFormatMetadataAsList +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TarantoolSpaceMetadata that = (TarantoolSpaceMetadata) o;
        return spaceId == that.spaceId &&
                ownerId == that.ownerId &&
                spaceName.equals(that.spaceName) &&
                Objects.equals(spaceFormatMetadata, that.spaceFormatMetadata) &&
                Objects.equals(spaceFormatMetadataAsList, that.spaceFormatMetadataAsList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(spaceId, ownerId, spaceName, spaceFormatMetadata, spaceFormatMetadataAsList);
    }

    /*
      - [281, 1, '_vspace', 'sysview', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'owner',
        'type': 'unsigned'}, {'name': 'name', 'type': 'string'}, {'name': 'engine',
        'type': 'string'}, {'name': 'field_count', 'type': 'unsigned'}, {'name': 'flags',
        'type': 'map'}, {'name': 'format', 'type': 'array'}]]
     */
}
