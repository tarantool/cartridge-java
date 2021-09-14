package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Ivan Dneprov
 */
public final class Utils {
    private Utils() {
    }

    /**
    * Checks if the space is empty.
    *
    * @param testSpace space to check
    */
    static void checkSpaceIsEmpty(TarantoolSpaceOperations<TarantoolTuple,
            TarantoolResult<TarantoolTuple>> testSpace) {
        assertEquals(0, testSpace.select(Conditions.any()).thenApply(List::size).join());
    }
}
