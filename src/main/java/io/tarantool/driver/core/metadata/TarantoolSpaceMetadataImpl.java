package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;

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
public class TarantoolSpaceMetadataImpl implements TarantoolSpaceMetadata {

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
    public TarantoolSpaceMetadataImpl() {
    }

    @Override
    public int getSpaceId() {
        return spaceId;
    }

    void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    @Override
    public int getOwnerId() {
        return ownerId;
    }

    void setOwnerId(int ownerId) {
        this.ownerId = ownerId;
    }

    @Override
    public String getSpaceName() {
        return spaceName;
    }

    void setSpaceName(String spaceName) {
        this.spaceName = spaceName;
    }

    @Override
    public Map<String, TarantoolFieldMetadata> getSpaceFormatMetadata() {
        return spaceFormatMetadata;
    }

    void setSpaceFormatMetadata(Map<String, TarantoolFieldMetadata> spaceFormatMetadata) {
        this.spaceFormatMetadata = spaceFormatMetadata;
        this.spaceFormatMetadataAsList = new ArrayList<>(spaceFormatMetadata.values());
    }

    @Override
    public Optional<TarantoolFieldMetadata> getFieldByName(String fieldName) {
        TarantoolFieldMetadata fieldMetadata = spaceFormatMetadata.get(fieldName);
        return Optional.ofNullable(fieldMetadata);
    }

    @Override
    public Optional<TarantoolFieldMetadata> getFieldByPosition(int fieldPosition) {
        if (fieldPosition >= spaceFormatMetadataAsList.size() || fieldPosition < 0) {
            return Optional.empty();
        }
        TarantoolFieldMetadata fieldMetadata = spaceFormatMetadataAsList.get(fieldPosition);
        return Optional.of(fieldMetadata);
    }

    @Override
    public int getFieldPositionByName(String fieldName) {
        return getFieldByName(fieldName).map(TarantoolFieldMetadata::getFieldPosition).orElse(-1);
    }

    @Override
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
        TarantoolSpaceMetadataImpl that = (TarantoolSpaceMetadataImpl) o;
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
