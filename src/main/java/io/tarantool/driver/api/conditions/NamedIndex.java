package io.tarantool.driver.api.conditions;

import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadataOperations;
import org.springframework.util.Assert;

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
    public TarantoolIndexMetadata metadata(TarantoolSpaceMetadataOperations spaceMetadataOperations) {
        return spaceMetadataOperations.getIndexByName(name);
    }

    @Override
    public String toIdentifier() {
        return name;
    }
}
