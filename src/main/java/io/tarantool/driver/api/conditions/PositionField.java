package io.tarantool.driver.api.conditions;

import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.springframework.util.Assert;

import java.util.Optional;

/**
 * Represents a field defined by position
 *
 * @author Alexey Kuzin
 */
public class PositionField implements FieldIdentifier<TarantoolFieldMetadata, Integer> {

    private int position;

    /**
     * Construct field using its position in field.
     *
     * @param position field position, starting from 0
     */
    public PositionField(int position) {
        Assert.state(position >= 0, "Field position must be greater or equal 0");

        this.position = position;
    }

    @Override
    public TarantoolFieldMetadata metadata(TarantoolMetadataOperations metadataOperations,
                                           TarantoolSpaceMetadata spaceMetadata) {

        Optional<TarantoolFieldMetadata> fieldMetadata = spaceMetadata.getFieldByPosition(position);
        if (!fieldMetadata.isPresent()) {
            throw new TarantoolFieldNotFoundException(position, spaceMetadata);
        }

        return fieldMetadata.get();
    }

    @Override
    public Integer toIdentifier() {
        return position;
    }
}
