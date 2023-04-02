package io.tarantool.driver.integration;


import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.List;

import static io.tarantool.driver.integration.Utils.checkSpaceIsEmpty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ClusterTruncateIT extends SharedTarantoolContainer {

    private static final String TEST_SPACE_NAME = "test_space";

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        startContainer();
        assertTrue(container.isRunning());
        initClient();
    }

    public static void tearDown() throws Exception {
        client.close();
        assertThrows(TarantoolClientException.class, () -> client.metadata().getSpaceByName("_space"));
    }

    @Test
    public void test_truncate2TimesOneSpace_shouldNotThrowExceptionsAndSpaceShouldBeEmptyAfterEtchCall() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        // call truncate then space is empty
        testSpace.truncate().join();
        checkSpaceIsEmpty(testSpace);

        for (int j = 0; j < 2; j++) {
            // prepare values to insert
            List<Object> values = Arrays.asList(1, "a", "Nineteen Eighty-Four", "George Orwell", 1984);
            TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());

            // then insert prepared values
            testSpace.insert(tarantoolTuple).join();

            // when values are inserted check that space isn't empty
            TarantoolResult<TarantoolTuple> selectResult = testSpace.select(Conditions.any()).join();
            assertEquals(1, selectResult.size());

            // after that truncate space
            testSpace.truncate().join();
            checkSpaceIsEmpty(testSpace);
        }
    }

    @Test
    public void test_truncateEmptySpace_shouldNotThrowExceptionsAndSpaceShouldBeEmpty() {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        // truncate space to make sure it is empty
        testSpace.truncate().join();

        // when truncate empty space and check that now exceptions was thrown and space is empty
        assertDoesNotThrow(() -> testSpace.truncate().join());
        checkSpaceIsEmpty(testSpace);
    }
}
