package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.cursor.TarantoolBatchCursorOptions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class StandaloneTarantoolCursorIT {
    private static final String TEST_SPACE_NAME = "cursor_test_space";
    private static final Logger log = LoggerFactory.getLogger(StandaloneTarantoolCursorIT.class);

    @Container
    private static final TarantoolContainer tarantoolContainer = new TarantoolContainer();

    private static TarantoolClient client;
    private static DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());
        initClient();

        try {
            insertTestData();
        } catch (ExecutionException | InterruptedException ignored) {
            fail();
        }
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
        client = new StandaloneTarantoolClient(config, serverAddress);
        log.info("Successfully connected to Tarantool, version = {}", client.getVersion());
    }

    private static void insertTestData() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        for (int i = 1; i <= 100; i++) {
            //insert new data
            List<Object> values = Arrays.asList(i, i + "abc", i * 10);
            TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
            testSpace.insert(tarantoolTuple).get();
        }
    }

    @Test
    public void getOneTuple() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(1));

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions());

        assertTrue(cursor.hasNext());
        TarantoolTuple tuple = cursor.next();
        assertFalse(cursor.hasNext());

        assertEquals(1, tuple.getInteger(0));
        assertEquals("1abc", tuple.getString(1));
        assertEquals(10, tuple.getInteger(2));

        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void countAllSmallBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions(3));

        assertTrue(cursor.hasNext());
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(100, countTotal);

        cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions(1));

        assertTrue(cursor.hasNext());
        countTotal = 0;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(100, countTotal);
    }

    @Test
    public void countAllSmallBatch2() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions(10));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAllBatch100() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions(100));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAllLargeBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(query, new TarantoolBatchCursorOptions(1000));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }
}
