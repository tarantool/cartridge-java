package io.tarantool.driver.integration;


import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import io.tarantool.driver.mappers.DefaultMessagePackMapper;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactory;
import io.tarantool.driver.mappers.TarantoolTupleResultMapperFactoryImpl;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.factories.ResultMapperFactoryFactoryImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ClusterTarantoolTupleClientIT {

    private static final String TEST_SPACE_NAME = "test_space";
    private static final Logger log = LoggerFactory.getLogger(ClusterTarantoolTupleClientIT.class);

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
    public void insertAndSelectRequests() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        //make select request
        Conditions conditions = Conditions.any();
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(conditions).get();

        int countBeforeInsert = selectResult.size();
        assertTrue(countBeforeInsert >= 2);

        TarantoolTuple tuple = selectResult.get(0);
        assertEquals(1, tuple.getInteger(0));
        assertEquals("a1", tuple.getString(1));
        assertEquals("Don Quixote", tuple.getString(2));
        assertEquals("Miguel de Cervantes", tuple.getString(3));
        assertEquals(1605, tuple.getInteger(4));

        //insert new data
        List<Object> values = Arrays.asList(4, "a4", "Nineteen Eighty-Four", "George Orwell", 1984);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.insert(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(5, insertTuples.get(0).size());
        assertEquals("George Orwell", insertTuples.get(0).getString(3));

        //repeat insert same data
        assertThrows(ExecutionException.class,
            () -> testSpace.insert(tarantoolTuple).get(),
            "Duplicate key exists in unique index 'primary' in space 'test_space'");

        //repeat select request
        TarantoolResult<TarantoolTuple> selectAfterInsertResult =
            testSpace.select(conditions).get();

        assertEquals(countBeforeInsert + 1, selectAfterInsertResult.size());

        TarantoolTuple newTuple = selectAfterInsertResult.stream()
            .filter(e -> e.getInteger(0) == 4)
            .findFirst().get();
        assertEquals(4, newTuple.getInteger(0));
        assertEquals("Nineteen Eighty-Four", newTuple.getString(2));
        assertEquals("George Orwell", newTuple.getString(3));
        assertEquals(1984, newTuple.getInteger(4));
    }

    @Test
    public void replaceRequest() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> arrayValue = Arrays.asList(200, 'a',
            "Harry Potter and the Philosopher's Stone", "J. K. Rowling", 1997);
        DefaultMessagePackMapper mapper = mapperFactory.defaultComplexTypesMapper();
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(arrayValue, mapper);
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.replace(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(5, insertTuples.get(0).size());
        assertEquals('a', insertTuples.get(0).getCharacter(1));
        assertEquals("J. K. Rowling", insertTuples.get(0).getString(3));

        //repeat insert same data
        assertDoesNotThrow(() -> testSpace.replace(tarantoolTuple).get());

        List<Object> newValues = Arrays.asList(200, 'a', "Jane Eyre", "Charlotte Brontë", 1847);
        TarantoolTuple newTupleWithSameId = new TarantoolTupleImpl(newValues, mapper);

        testSpace.replace(newTupleWithSameId);

        //select request
        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(200));
        TarantoolResult<TarantoolTuple> selectAfterReplaceResult = testSpace.select(conditions).get();

        assertEquals(1, selectAfterReplaceResult.size());

        Optional<TarantoolTuple> value = selectAfterReplaceResult.stream().findFirst();
        assertTrue(value.isPresent());

        assertEquals(200, value.get().getInteger(0));
        assertEquals('a', insertTuples.get(0).getCharacter("unique_key"));
        assertEquals("Jane Eyre", value.get().getString(2));
        assertEquals("Charlotte Brontë", value.get().getString(3));
        assertEquals(1847, value.get().getInteger(4));
    }

    @Test
    public void test_insertMany_shouldThrowException() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        assertThrows(UnsupportedOperationException.class,
            () -> testSpace.insertMany(Collections.emptyList()));
    }

    @Test
    public void test_replaceMany_shouldThrowException() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        assertThrows(UnsupportedOperationException.class,
            () -> testSpace.replaceMany(Collections.emptyList()));
    }

    @Test
    public void deleteRequest() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);
        int deletedId = 2;

        //select request
        Conditions conditions = Conditions.equals("id", deletedId);
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(conditions).get();

        Optional<TarantoolTuple> value = selectResult.stream().findFirst();
        assertEquals(1, selectResult.size());
        assertTrue(value.isPresent());

        //delete tuple by id
        TarantoolResult<TarantoolTuple> deleteRequestResult = testSpace.delete(conditions).get();
        assertEquals(1, deleteRequestResult.size());
        assertEquals(deletedId, deleteRequestResult.get(0).getInteger(0));

        //select after delete request
        TarantoolResult<TarantoolTuple> selectAfterDeleteResult = testSpace.select(conditions).get();

        Optional<TarantoolTuple> deletedValue = selectAfterDeleteResult.stream()
            .filter(v -> v.getInteger(0) == deletedId).findFirst();
        assertEquals(0, selectAfterDeleteResult.size());
        assertFalse(deletedValue.isPresent());
    }

    @Test
    public void updateOperationsTest() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(123, "a123", "The Lord of the Rings", "J. R. R. Tolkien", 1968);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());
        testSpace.insert(tarantoolTuple).join();

        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(123));

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = testSpace.update(conditions, TupleOperations.add(4, 100000)).get();
        assertEquals(101968, updateResult.get(0).getInteger(4));

        updateResult = testSpace.update(conditions, TupleOperations.subtract(4, 50000)).get();
        assertEquals(51968, updateResult.get(0).getInteger(4));

        updateResult = testSpace.update(conditions, TupleOperations.set(4, 10)).get();
        assertEquals(10, updateResult.get(0).getInteger(4));

        updateResult = testSpace.update(conditions, TupleOperations.bitwiseXor(4, 10)).get();
        assertEquals(0, updateResult.get(0).getInteger(4));

        updateResult = testSpace.update(conditions, TupleOperations.bitwiseOr(4, 5)).get();
        assertEquals(5, updateResult.get(0).getInteger(4));

        updateResult = testSpace.update(conditions, TupleOperations.bitwiseAnd(4, 6)).get();
        assertEquals(4, updateResult.get(0).getInteger(4));

        // set multiple fields
        updateResult = testSpace.update(conditions, TupleOperations.set(3, "new string").andSet(4, 999)).get();
        assertEquals("new string", updateResult.get(0).getString(3));
        assertEquals(999, updateResult.get(0).getInteger(4));
        assertEquals(5, updateResult.get(0).size());

        updateResult = testSpace.update(conditions, TupleOperations.delete(4, 1)).get();
        assertEquals(4, updateResult.get(0).size());

        updateResult = testSpace.update(conditions, TupleOperations.insert(4, 1888)).get();
        assertEquals(5, updateResult.get(0).size());
    }

    @Test
    public void updateOperationByUniqueIndexTest() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(166, "a166", "The Lord of the Rings", "J. R. R. Tolkien", 1968);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());
        testSpace.insert(tarantoolTuple);

        Conditions conditions = Conditions.equals("id", 166);

        TarantoolResult<TarantoolTuple> updateResult;

        updateResult = testSpace.update(conditions, TupleOperations.add(4, 10)).get();
        assertEquals(1978, updateResult.get(0).getInteger(4));

        //First from the end
        updateResult = testSpace.update(conditions, TupleOperations.add(-1, 10)).get();
        assertEquals(1988, updateResult.get(0).getInteger(4));

        //Update by unique index
        Conditions queryByUniqIndex = Conditions.indexEquals(2, Collections.singletonList("a166"));

        updateResult = testSpace.update(queryByUniqIndex, TupleOperations.add(4, 12)).get();
        assertEquals(2000, updateResult.get(0).getInteger(4));

        //Update by not unique index
        Conditions queryByNotUniqIndex = Conditions.indexEquals(1, Collections.singletonList("J. R. R. Tolkien"));

        assertThrows(TarantoolSpaceOperationException.class, () -> testSpace.update(queryByNotUniqIndex,
            TupleOperations.add(4, 12)).join());
    }

    @Test
    public void updateByFieldName() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> newValues =
            Arrays.asList(105534, "a120654", "The Jungle Book", "Sir Joseph Rudyard Kipling", 1893);
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());
        testSpace.insert(tarantoolTuple);

        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(105534));

        TarantoolResult<TarantoolTuple> updateResult;
        TupleOperations op = TupleOperations.add("year", 7);
        assertNull(op.asList().get(0).getFieldIndex());

        updateResult = testSpace.update(conditions, op).get();
        // this check is needed to understand that fieldIndex does not change after the operation
        assertNull(op.asList().get(0).getFieldIndex());
        assertEquals(1900, updateResult.get(0).getInteger(4));

        // an attempt to update by the name of a non-existed field
        assertThrows(TarantoolSpaceFieldNotFoundException.class,
            () -> testSpace.update(conditions,
                TupleOperations.add("non-existed-field", 17)).get());
    }

    @Test
    public void upsertTest() throws Exception {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace =
            client.space(TEST_SPACE_NAME);

        List<Object> newValues = Arrays.asList(255, "q255", "Animal Farm: A Fairy Story", "George Orwell", 1945);

        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());

        Conditions conditions = Conditions.equals("id", 255);

        TupleOperations ops = TupleOperations.set(4, 2020)
            .andSplice(2, 5, 1, "aaa")
            .andSet("author", "Leo Tolstoy");
        assertNull(ops.asList().get(2).getFieldIndex());

        // run upsert first time tuple
        testSpace.upsert(conditions, tarantoolTuple, ops).get();
        // this check is needed to understand that fieldIndex does not change after the operation
        assertNull(ops.asList().get(2).getFieldIndex());

        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(conditions).get();

        assertEquals(1, selectResult.size());
        assertEquals(1945, selectResult.get(0).getInteger(4));
        assertEquals("Animal Farm: A Fairy Story", selectResult.get(0).getString(2));
        assertEquals("George Orwell", selectResult.get(0).getString("author"));

        // run upsert second time
        testSpace.upsert(conditions, tarantoolTuple, ops).get();
        // this check is needed to understand that fieldIndex does not change after the operation
        assertNull(ops.asList().get(2).getFieldIndex());

        selectResult = testSpace.select(conditions).get();
        assertEquals(1, selectResult.size());
        assertEquals(2020, selectResult.get(0).getInteger(4));
        assertEquals("Animaaaa Farm: A Fairy Story", selectResult.get(0).getString(2));
        assertEquals("Leo Tolstoy", selectResult.get(0).getString("author"));

        // an attempt to upsert by the name of a non-existed field
        assertThrows(TarantoolSpaceFieldNotFoundException.class,
            () -> testSpace.upsert(conditions, tarantoolTuple,
                TupleOperations.set("non-existed-field", 17)).get());
    }

    @Test
    public void callTest() throws Exception {
        List<?> resultNoParam = client.call("user_function_no_param").get();

        assertEquals(1, resultNoParam.size());
        assertEquals(5, resultNoParam.get(0));

        List<?> resultTwoParams = client.call("user_function_two_param", Arrays.asList(1, "abc")).get();

        assertEquals(3, resultTwoParams.size());
        assertEquals(1, resultTwoParams.get(0));
        assertEquals("abc", resultTwoParams.get(1));
        assertEquals("Hello, 1 abc", resultTwoParams.get(2));
    }

    @Test
    public void callForTarantoolResultTest() throws Exception {
        MessagePackMapper defaultMapper = client.getConfig().getMessagePackMapper();
        TarantoolTupleResultMapperFactory factory =
            TarantoolTupleResultMapperFactoryImpl.getInstance();
        TarantoolSpaceMetadata spaceMetadata = client.metadata().getSpaceByName("test_space").get();
        TarantoolResult<TarantoolTuple> result = client.call(
            "user_function_complex_query",
            Collections.singletonList(1000),
            defaultMapper,
            factory.withSingleValueArrayToTarantoolTupleResultMapper(defaultMapper, spaceMetadata)
        ).get();

        assertTrue(result.size() >= 3);
        assertEquals(1605, result.get(0).getInteger("year"));
    }

    @Test
    public void evalTest() throws ExecutionException, InterruptedException {
        List<?> result = client.eval("return 2+2").get();

        assertEquals(1, result.size());
        assertEquals(4, result.get(0));

        result = client.eval("return 5*5, 'abc'").get();

        assertEquals(2, result.size());
        assertEquals(25, result.get(0));
        assertEquals("abc", result.get(1));
    }

    @Test
    public void evalReturnNil() throws ExecutionException, InterruptedException {
        List<?> result = client.eval("return 2.2 + 2, nil").get();

        assertEquals(2, result.size());

        MathContext mathContext = new MathContext(1, RoundingMode.HALF_UP);
        assertEquals(0, new BigDecimal("4.2", mathContext)
            .compareTo(new BigDecimal(String.valueOf(result.get(0)), mathContext)));
    }

    @Test
    public void testCallReturnLongValue() throws Exception {
        client.getVersion();
        List<?> result = client.call("user_function_return_long_value").get();

        assertEquals(1, result.size());
    }
}
