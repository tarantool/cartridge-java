package io.tarantool.driver.metadata;

/**
 * Represents Tarantool space metadata (space ID, space name, etc.)
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceMetadata {

    private int spaceId;
    private int ownerId;
    private String spaceName;
    //TODO private TarantoolEngine engine;
    //TODO format

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

    /*
      - [281, 1, '_vspace', 'sysview', 0, {}, [{'name': 'id', 'type': 'unsigned'}, {'name': 'owner',
        'type': 'unsigned'}, {'name': 'name', 'type': 'string'}, {'name': 'engine',
        'type': 'string'}, {'name': 'field_count', 'type': 'unsigned'}, {'name': 'flags',
        'type': 'map'}, {'name': 'format', 'type': 'array'}]]
     */
}
