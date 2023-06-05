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

    private final Object name;

    /**
     * Construct index by its name
     *
     * @param name index name, should not be empty
     */
    public NamedIndexImpl(Object name) {
        if (name instanceof String) {
            Assert.hasText((String) name, "Index name should be not empty String or Integer");
        } else if (!(name instanceof Integer)) {
            throw new IllegalArgumentException("Index name should be not empty String or Integer");
        }

        this.name = name;
    }

    @Override
    public TarantoolIndexMetadata metadata(
        TarantoolMetadataOperations metadataOperations,
        TarantoolSpaceMetadata spaceMetadata) {
        Optional<TarantoolIndexMetadata> indexMetadata =
            metadataOperations.getIndexByName(spaceMetadata.getSpaceName(), name);
        if (!indexMetadata.isPresent()) {
            throw new TarantoolIndexNotFoundException(spaceMetadata.getSpaceName(), name);
        }

        return indexMetadata.get();
    }

    @Override
    public Object toIdentifier() {
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
