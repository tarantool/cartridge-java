package io.tarantool.driver.exceptions;

import io.tarantool.driver.TarantoolClientException;

/**
 * Represents index not found by name error
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexNotFoundException extends TarantoolClientException {

    public TarantoolIndexNotFoundException(int spaceId, String indexName) {
        super(String.format("Index '%s' is not found in space %d", indexName, spaceId));
    }
}
