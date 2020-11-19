package io.tarantool.driver.api.conditions;

import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.metadata.TarantoolMetadataOperations;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.utils.Assert;

import java.util.Optional;

/**
 * Represents a field defined by name
 *
 * @author Alexey Kuzin
 */
public class NamedField implements FieldIdentifier<TarantoolFieldMetadata, String> {

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
}
