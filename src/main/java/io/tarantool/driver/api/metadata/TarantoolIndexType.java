package io.tarantool.driver.api.metadata;

import static java.lang.String.format;

/**
 * Represents all types of Tarantool space indexes.
 *
 * @author Sergey Volgin
 */
public enum TarantoolIndexType {
    HASH("HASH"),
    TREE("TREE"),
    BITSET("BITSET"),
    RTREE("RTREE");

    private final String name;

    TarantoolIndexType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns the TarantoolIndexType from the string value.
     *
     * @param indexType the string value.
     * @return the Tarantool index type
     */
    public static TarantoolIndexType fromString(final String indexType) {
        for (TarantoolIndexType index : TarantoolIndexType.values()) {
            if (index.getName().equalsIgnoreCase(indexType)) {
                return index;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid TarantoolIndexType", indexType));
    }
}
