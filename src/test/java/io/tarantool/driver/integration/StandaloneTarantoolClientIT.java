package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class StandaloneTarantoolClientIT {

    private static Logger log = LoggerFactory.getLogger(StandaloneTarantoolClientIT.class);

    private static final String TEST_SPACE_NAME = "test_space";

    @Container
    private static final TarantoolContainer tarantoolContainer = new TarantoolContainer();

    private static TarantoolConnection connection;

    private static DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() throws TarantoolClientException {
        assertTrue(tarantoolContainer.isRunning());
        initConnection();
    }

    private static void initConnection() throws TarantoolClientException {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                //"guest", "");
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 5)
                .withReadTimeout(1000 * 5)
                .withRequestTimeout(1000 * 5)
                .build();

        log.info("Attempting connect to Tarantool");
        connection = new StandaloneTarantoolClient(config)
                .connect(tarantoolContainer.getHost(), tarantoolContainer.getPort());
        log.info("Successfully connected to Tarantool, version = {}", connection.getVersion());
    }

    @Test
    public void connectAndCheckMetadata() throws TarantoolClientException {
        Optional<TarantoolSpaceMetadata> spaceHolder = connection.metadata().getSpaceByName("_space");
        assertTrue(spaceHolder.isPresent(), "Failed to get space metadata");
        log.info("Retrieved ID from metadata for space '_space': {}",
                spaceHolder.get().getSpaceId());

        Optional<TarantoolSpaceMetadata> spaceMetadata = connection.metadata().getSpaceByName(TEST_SPACE_NAME);
        assertTrue(spaceMetadata.isPresent(), String.format("Failed to get '%s' metadata", TEST_SPACE_NAME));
        assertEquals(TEST_SPACE_NAME, spaceMetadata.get().getSpaceName());
        log.info("Retrieved ID from metadata for space '{}': {}",
                spaceMetadata.get().getSpaceName(), spaceMetadata.get().getSpaceId());
    }

    //TODO: reset space before each test
    @Test
    public void insertAndSelectRequests() throws TarantoolClientException, Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);
        //make select request
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        int countBeforeInsert = selectResult.size();
        assertTrue(countBeforeInsert >= 2);

        TarantoolTuple tuple = selectResult.get(0);
        assertEquals(1, tuple.getInteger(0));
        assertEquals("Don Quixote", tuple.getString(1));
        assertEquals("Miguel de Cervantes", tuple.getString(2));
        assertEquals(1605, tuple.getInteger(3));

        //insert new data
        List<Object> values = Arrays.asList(4, "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.insert(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(4, insertTuples.get(0).size());
        assertEquals("George Orwell", insertTuples.get(0).getString(2));

        //repeat insert same data
        assertThrows(ExecutionException.class,
                 () -> testSpace.insert(tarantoolTuple).get(),
        "Duplicate key exists in unique index 'primary' in space 'test_space'");

        //repeat select request
        TarantoolResult<TarantoolTuple> selectAfterInsertResult =
                testSpace.select(query, new TarantoolSelectOptions()).get();

        assertEquals(countBeforeInsert + 1, selectAfterInsertResult.size());

        TarantoolTuple newTuple = selectAfterInsertResult.stream()
                .filter(e -> e.getInteger(0) == 4)
                .findFirst().get();
        assertEquals(4, newTuple.getInteger(0));
        assertEquals("Nineteen Eighty-Four", newTuple.getString(1));
        assertEquals("George Orwell", newTuple.getString(2));
        assertEquals(1984, newTuple.getInteger(3));
    }

    @Test
    public void replaceRequest() throws TarantoolClientException, Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);

        List<Object> arrayValue = Arrays.asList(200, "Harry Potter and the Philosopher's Stone", "J. K. Rowling", 1997);
        DefaultMessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(arrayValue, mapper);
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.replace(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(4, insertTuples.get(0).size());
        assertEquals("J. K. Rowling", insertTuples.get(0).getString(2));

        //repeat insert same data
        assertDoesNotThrow(() -> testSpace.replace(tarantoolTuple).get());

        List<Object> newValues = Arrays.asList(200L, "Jane Eyre", "Charlotte Brontë", 1847);
        TarantoolTuple newTupleWithSameId = new TarantoolTupleImpl(newValues, mapper);

        testSpace.replace(newTupleWithSameId);

        //select request
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(200));
        TarantoolResult<TarantoolTuple> selectAfterReplaceResult =
                testSpace.select(query, new TarantoolSelectOptions()).get();

        assertEquals(1, selectAfterReplaceResult.size());

        Optional<TarantoolTuple> value = selectAfterReplaceResult.stream().findFirst();
        assertTrue(value.isPresent());

        assertEquals(200, value.get().getInteger(0));
        assertEquals("Jane Eyre", value.get().getString(1));
        assertEquals("Charlotte Brontë", value.get().getString(2));
        assertEquals(1847, value.get().getInteger(3));
    }

    @Test
    public void deleteRequest() throws TarantoolClientException, Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);
        int deletedId = 2;

        //select request
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(deletedId));
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        Optional<TarantoolTuple> value = selectResult.stream().findFirst();
        assertEquals(1, selectResult.size());
        assertTrue(value.isPresent());

        //delete tuple by id
        TarantoolResult<TarantoolTuple> deleteRequestResult = testSpace.delete(query).get();
        assertEquals(1, deleteRequestResult.size());
        assertEquals(deletedId, deleteRequestResult.get(0).getInteger(0));

        //select after delete request
        TarantoolResult<TarantoolTuple> selectAfterDeleteResult =
                testSpace.select(query, new TarantoolSelectOptions()).get();

        Optional<TarantoolTuple> deletedValue = selectAfterDeleteResult.stream()
                .filter(v -> v.getInteger(0) == deletedId).findFirst();
        assertEquals(0, selectAfterDeleteResult.size());
        assertFalse(deletedValue.isPresent());
    }

    @Test
    public void callTest() throws Exception, TarantoolClientException {
        List<Object> resultNoParam = connection.call("user_function_no_param").get();

        assertEquals(1, resultNoParam.size());
        assertEquals(5, resultNoParam.get(0));

        List<Object> resultTwoParams =
                connection.call("user_function_two_param", Arrays.asList(1, "abc")).get();

        assertEquals(3, resultTwoParams.size());
        assertEquals("Hello, 1 abc", resultTwoParams.get(2));
    }
}
