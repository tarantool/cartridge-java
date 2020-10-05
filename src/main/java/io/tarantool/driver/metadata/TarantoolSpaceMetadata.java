package io.tarantool.driver.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

/**
 * Represents Tarantool space metadata (space ID, space name, etc.)
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceMetadata {

    private int spaceId;
    private int ownerId;
    private String spaceName;
    private LinkedHashMap<String, TarantoolFieldFormatMetadata> spaceFormatMetadata;
    private List<TarantoolFieldFormatMetadata> spaceFormatMetadataAsList;
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

    public LinkedHashMap<String, TarantoolFieldFormatMetadata> getSpaceFormatMetadata() {
        return spaceFormatMetadata;
    }

    void setSpaceFormatMetadata(LinkedHashMap<String, TarantoolFieldFormatMetadata> spaceFormatMetadata) {
        this.spaceFormatMetadata = spaceFormatMetadata;
        this.spaceFormatMetadataAsList = new ArrayList<>(spaceFormatMetadata.values());
    }

    /**
     * Get field position in space by name starts with 0, or -1 if this field not found in format metadata
     *
     * @param fieldName field name
     * @return field position by name starting with 0, or -1 if this field not found in format metadata
     */
    public int getFieldPositionByName(String fieldName) {
        int fieldPosition = -1;
        if (spaceFormatMetadata.containsKey(fieldName)) {
            fieldPosition = spaceFormatMetadata.get(fieldName).getFieldPosition();
        }

        return  fieldPosition;
    }

    /**
     * Get field name in space by positions starting with 0
     *
     * @param fieldPosition field position starting with 0
     * @return field name or null if this field not found in format metadata
     */
    public Optional<String> getFieldNameByPosition(int fieldPosition) {
        TarantoolFieldFormatMetadata fieldFormatMetadata = spaceFormatMetadataAsList.get(fieldPosition);
        if (fieldFormatMetadata == null) {
            return Optional.empty();
        }
        return Optional.of(fieldFormatMetadata.getFieldName());
    }

    /*
      - [281, 1, '_vspace', 'sysview', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'owner',
        'type': 'unsigned'}, {'name': 'name', 'type': 'string'}, {'name': 'engine',
        'type': 'string'}, {'name': 'field_count', 'type': 'unsigned'}, {'name': 'flags',
        'type': 'map'}, {'name': 'format', 'type': 'array'}]]
     */
}
