package io.tarantool.driver.api.conditions;

import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.core.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.core.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Objects;
import java.util.Optional;

/**
 * Represents a field defined by name
 *
 * @author Alexey Kuzin
 */
public class NamedField implements FieldIdentifier<TarantoolFieldMetadata, String> {

    private static final long serialVersionUID = 20200708L;

    private String name;

    /**
     * Construct field using its name.
     *
     * @param name field name, should not be empty
     */
    public NamedField(String name) {
        Assert.hasText(name, "Field name should not be empty");

        this.name = name;
    }

    @Override
    public TarantoolFieldMetadata metadata(TarantoolMetadataOperations metadataOperations,
                                           TarantoolSpaceMetadata spaceMetadata) {

        Optional<TarantoolFieldMetadata> fieldMetadata = spaceMetadata.getFieldByName(name);
        if (!fieldMetadata.isPresent()) {
            throw new TarantoolFieldNotFoundException(name, spaceMetadata);
        }

        return fieldMetadata.get();
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
        NamedField that = (NamedField) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
