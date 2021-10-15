package io.tarantool.driver.core.conditions;

import io.tarantool.driver.api.conditions.NamedIndex;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.utils.Assert;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents an index defined by name
 *
 * @author Alexey Kuzin
 */
public class NamedIndexImpl implements NamedIndex {

    private static final long serialVersionUID = 20200708L;

    private final String name;

    /**
     * Construct index by its name
     *
     * @param name index name, should not be empty
     */
    public NamedIndexImpl(String name) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NamedIndexImpl that = (NamedIndexImpl) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
