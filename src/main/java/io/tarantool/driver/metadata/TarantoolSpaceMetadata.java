package io.tarantool.driver.metadata;

import java.util.LinkedHashMap;

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

    /*
      - [281, 1, '_vspace', 'sysview', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'owner',
        'type': 'unsigned'}, {'name': 'name', 'type': 'string'}, {'name': 'engine',
        'type': 'string'}, {'name': 'field_count', 'type': 'unsigned'}, {'name': 'flags',
        'type': 'map'}, {'name': 'format', 'type': 'array'}]]
     */
}
