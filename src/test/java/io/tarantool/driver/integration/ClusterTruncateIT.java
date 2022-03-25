package io.tarantool.driver.integration;


import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.List;

import static io.tarantool.driver.integration.Utils.checkSpaceIsEmpty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ClusterTruncateIT {

    private static final String TEST_SPACE_NAME = "test_space";
    private static final Logger log = LoggerFactory.getLogger(ClusterTruncateIT.class);

    @Container
    private static final TarantoolContainer tarantoolContainer = new TarantoolContainer()
            .withScriptFileName("org/testcontainers/containers/server.lua")
            .withLogConsumer(new Slf4jLogConsumer(log));

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());
        initClient();
    }

    public static void tearDown() throws Exception {
        client.close();
        assertThrows(TarantoolClientException.class, () -> client.metadata().getSpaceByName("_space"));
    }

    private static void initClient() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolServerAddress serverAddress = new TarantoolServerAddress(
                tarantoolContainer.getHost(), tarantoolContainer.getPort());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        log.info("Attempting connect to Tarantool");
        client = new ClusterTarantoolTupleClient(config, serverAddress);
        log.info("Successfully connected to Tarantool, version = {}", client.getVersion());
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
