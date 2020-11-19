package io.tarantool.driver.api.conditions;

import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Optional;

/**
 * Represents an index defined by its id
 *
 * @author Alexey Kuzin
 */
public class IdIndex implements FieldIdentifier<TarantoolIndexMetadata, Integer> {

    private int position;

    /**
     * Construct index by its id
     *
     * @param position index position in the order of creation, starting from 0
     */
    public IdIndex(int position) {
        Assert.state(position >= 0, "Index position should be greater or equal 0");

        this.position = position;
    }

    @Override
    public TarantoolIndexMetadata metadata(TarantoolMetadataOperations metadataOperations,
                                           TarantoolSpaceMetadata spaceMetadata) {
        Optional<TarantoolIndexMetadata> indexMetadata =
                metadataOperations.getIndexById(spaceMetadata.getSpaceName(), position);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceMetadata.getSpaceName(), position);
        }

        return indexMetadata.get();
    }

    @Override
    public Integer toIdentifier() {
        return position;
    }
}
