package io.tarantool.driver.core.conditions;

import io.tarantool.driver.api.conditions.IdIndex;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.utils.Assert;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents an index defined by its id
 *
 * @author Alexey Kuzin
 */
public class IdIndexImpl implements IdIndex {

    private static final long serialVersionUID = 20200708L;

    private final int position;

    /**
     * Construct index by its id
     *
     * @param position index position in the order of creation, starting from 0
     */
    public IdIndexImpl(int position) {
        Assert.state(position >= 0, "Index position should be greater or equal 0");

        this.position = position;
    }

    @Override
    public TarantoolIndexMetadata metadata(
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata) {
        Optional<TarantoolIndexMetadata> indexMetadata =
            metadataOperations.getIndexById(spaceMetadata.getSpaceName(), position);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceMetadata.getSpaceName(), position);
        }

        return indexMetadata.get();
    }

    @Override
    public Object toIdentifier() {
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
        IdIndexImpl idIndex = (IdIndexImpl) o;
        return position == idIndex.position;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position);
    }
}
