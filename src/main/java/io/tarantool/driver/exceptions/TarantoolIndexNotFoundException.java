package io.tarantool.driver.exceptions;

/**
 * Represents index not found by name error
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexNotFoundException extends TarantoolException {

    public TarantoolIndexNotFoundException(int spaceId, Object indexName) {
        super(String.format("Index '%s' is not found in space %d", indexName, spaceId));
    }

    public TarantoolIndexNotFoundException(String spaceName, Object indexName) {
        super(String.format("Index '%s' is not found in space %s", indexName, spaceName));
    }

    public TarantoolIndexNotFoundException(String spaceName, int indexId) {
        super(String.format("Index with id %d is not found in space %s", indexId, spaceName));
    }
}
