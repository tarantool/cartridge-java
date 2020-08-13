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
import io.tarantool.driver.protocol.operations.TupleOperations;
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
    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());
        initConnection();
    }

    private static void initConnection() {
        TarantoolCredentials credentials = new SimpleTarantoolCredentials(
                //"guest", "");
                tarantoolContainer.getUsername(), tarantoolContainer.getPassword());

        TarantoolClientConfig config = new TarantoolClientConfig.Builder()
                .withCredentials(credentials)
                .withConnectTimeout(1000 * 500)
                .withReadTimeout(1000 * 500)
                .withRequestTimeout(1000 * 500)
                .build();

        log.info("Attempting connect to Tarantool");
        connection = new StandaloneTarantoolClient(config)
                .connect(tarantoolContainer.getHost(), tarantoolContainer.getPort());
        log.info("Successfully connected to Tarantool, version = {}", connection.getVersion());
    }

    @Test
    public void connectAndCheckMetadata() {
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
    public void insertAndSelectRequests() throws Exception {
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
    public void replaceRequest() throws Exception {
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
    public void deleteRequest() throws Exception {
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
    public void updateTest() throws Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(123, "The Lord of the Rings", "J. R. R. Tolkien", 1968);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());
        testSpace.insert(tarantoolTuple);

        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(123));

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = testSpace.update(query, TupleOperations.add(3, 100000)).get();
        assertEquals(101968, updateResult.get(0).getInteger(3));

        updateResult = testSpace.update(query, TupleOperations.subtract(3, 50000)).get();
        assertEquals(51968, updateResult.get(0).getInteger(3));

        updateResult = testSpace.update(query, TupleOperations.set(3, 10)).get();
        assertEquals(10, updateResult.get(0).getInteger(3));

        updateResult = testSpace.update(query, TupleOperations.bitwiseXor(3, 10)).get();
        assertEquals(0, updateResult.get(0).getInteger(3));

        updateResult = testSpace.update(query, TupleOperations.bitwiseOr(3, 5)).get();
        assertEquals(5, updateResult.get(0).getInteger(3));

        updateResult = testSpace.update(query, TupleOperations.bitwiseAnd(3, 6)).get();
        assertEquals(4, updateResult.get(0).getInteger(3));

        // set multiple fields
        updateResult = testSpace.update(query, TupleOperations.set(2, "new string").andSet(3, 999)).get();
        assertEquals("new string", updateResult.get(0).getString(2));
        assertEquals(999, updateResult.get(0).getInteger(3));
        assertEquals(4, updateResult.get(0).size());

        updateResult = testSpace.update(query, TupleOperations.delete(3, 1)).get();
        assertEquals(3, updateResult.get(0).size());

        updateResult = testSpace.update(query, TupleOperations.insert(3, 1888)).get();
        assertEquals(4, updateResult.get(0).size());
    }

    @Test
    public void updateByFieldName() throws Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(105534, "The Jungle Book", "Sir Joseph Rudyard Kipling", 1893);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());
        testSpace.insert(tarantoolTuple);

        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(105534));

        TarantoolResult<TarantoolTuple> updateResult;
        updateResult = testSpace.update(query, TupleOperations.add("year", 7)).get();
        assertEquals(1900, updateResult.get(0).getInteger(3));
    }

    @Test
    public void upsertTest() throws Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(255, "Animal Farm: A Fairy Story", "George Orwell", 1945);

        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());

        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(255));

        TupleOperations ops = TupleOperations.set(3, 2020)
                .andSplice(1, 5, 1, "aaa");

        //run upsert first time tuple
        TarantoolResult<TarantoolTuple> upsertResult = testSpace.upsert(query, tarantoolTuple, ops).get();

        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        assertEquals(1, selectResult.size());
        assertEquals(1945, selectResult.get(0).getInteger(3));
        assertEquals("Animal Farm: A Fairy Story", selectResult.get(0).getString(1));

        //run upsert second time
        upsertResult = testSpace.upsert(query, tarantoolTuple, ops).get();

        selectResult = testSpace.select(query, new TarantoolSelectOptions()).get();
        assertEquals(1, selectResult.size());
        assertEquals(2020, selectResult.get(0).getInteger(3));
        assertEquals("Animaaaa Farm: A Fairy Story", selectResult.get(0).getString(1));
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
