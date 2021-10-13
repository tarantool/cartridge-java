package io.tarantool.driver.core.conditions;

import io.tarantool.driver.api.conditions.PositionField;
import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a field defined by position
 *
 * @author Alexey Kuzin
 */
public class PositionFieldImpl implements PositionField {

    private static final long serialVersionUID = 20200708L;

    private final int position;

    /**
     * Construct field using its position in field.
     *
     * @param position field position, starting from 0
     */
    public PositionFieldImpl(int position) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PositionFieldImpl that = (PositionFieldImpl) o;
        return position == that.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}
