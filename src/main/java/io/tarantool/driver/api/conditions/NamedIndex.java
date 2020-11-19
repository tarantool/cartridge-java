package io.tarantool.driver.api.conditions;

import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Optional;

/**
 * Represents an index defined by name
 *
 * @author Alexey Kuzin
 */
public class NamedIndex implements FieldIdentifier<TarantoolIndexMetadata, String> {

    private final String name;

    /**
     * Construct index by its name
     *
     * @param name index name, should not be empty
     */
    public NamedIndex(String name) {
        Assert.hasText(name, "Index name should not be empty");

        this.name = name;
    }

    @Override
    public TarantoolIndexMetadata metadata(TarantoolMetadataOperations metadataOperations,
                                           TarantoolSpaceMetadata spaceMetadata) {
        Optional<TarantoolIndexMetadata> indexMetadata =
                metadataOperations.getIndexByName(spaceMetadata.getSpaceName(), name);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceMetadata.getSpaceName(), name);
        }

        return indexMetadata.get();
    }

    @Override
    public String toIdentifier() {
        return name;
    }
}
