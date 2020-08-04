package io.tarantool.driver.integration;

import io.tarantool.driver.StandaloneTarantoolClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolClientException;
import io.tarantool.driver.TarantoolConnection;
import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.TarantoolSelectOptions;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.TarantoolContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collections;
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
        assertTrue(connection.metadata().getSpaceByName("_space").isPresent(), "Failed to get space metadata");
        log.info("Retrieved ID from metadata for space '_space': {}",
                connection.metadata().getSpaceByName("_space").get().getSpaceId());

        Optional<TarantoolSpaceMetadata> spaceMetadata = connection.metadata().getSpaceByName(TEST_SPACE_NAME);
        assertTrue(spaceMetadata.isPresent(), String.format("Failed to get '%s' metadata", TEST_SPACE_NAME));
        assertEquals(TEST_SPACE_NAME, spaceMetadata.get().getSpaceName());
        log.info("Retrieved ID from metadata for space '{}': {}", spaceMetadata.get().getSpaceName(), spaceMetadata.get().getSpaceId());
    }

    @Test
    public void insertAndSelectRequests() throws TarantoolClientException, Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);
        //make select request
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        TarantoolResult<TarantoolTuple> selectResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        int countBeforeInsert = selectResult.size();
        assertTrue(countBeforeInsert >= 3);

        TarantoolTuple tuple = selectResult.get(0);
        assertEquals(1, tuple.getField(0).get().getInteger());
        assertEquals("Don Quixote", tuple.getField(1).get().getString());
        assertEquals("Miguel de Cervantes", tuple.getField(2).get().getString());
        assertEquals(1605, tuple.getField(3).get().getInteger());

        //insert new data
        ImmutableArrayValue values = ValueFactory.newArray(
                new ImmutableLongValueImpl(4L),
                new ImmutableStringValueImpl("Nineteen Eighty-Four"),
                new ImmutableStringValueImpl("George Orwell"),
                new ImmutableLongValueImpl(1984L)
        );
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.insert(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(4, insertTuples.get(0).size());
        assertEquals("George Orwell", insertTuples.get(0).getField(2).get().getString());

        //repeat insert same data
         assertThrows(ExecutionException.class,
                 () -> testSpace.insert(tarantoolTuple).get(),
        "Duplicate key exists in unique index 'primary' in space 'test_space'");

        //repeat select request
        TarantoolResult<TarantoolTuple> selectAfterInsertResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        assertEquals(countBeforeInsert + 1, selectAfterInsertResult.size());

        TarantoolTuple newTuple = selectAfterInsertResult.stream().filter(e -> e.getField(0).get().getInteger() == 4).findFirst().get();
        assertEquals(4, newTuple.getField(0).get().getInteger());
        assertEquals("Nineteen Eighty-Four", newTuple.getField(1).get().getString());
        assertEquals("George Orwell", newTuple.getField(2).get().getString());
        assertEquals(1984, newTuple.getField(3).get().getInteger());
    }

    @Test
    public void replaceRequest() throws TarantoolClientException, Exception {
        TarantoolSpaceOperations testSpace = connection.space(TEST_SPACE_NAME);

        ImmutableArrayValue arrayValue = ValueFactory.newArray(
                new ImmutableLongValueImpl(200L),
                new ImmutableStringValueImpl("Harry Potter and the Philosopher's Stone"),
                new ImmutableStringValueImpl("J. K. Rowling"),
                new ImmutableLongValueImpl(1997L)
        );
        TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(arrayValue, mapperFactory.defaultComplexTypesMapper());
        TarantoolResult<TarantoolTuple> insertTuples = testSpace.replace(tarantoolTuple).get();

        assertEquals(1, insertTuples.size());
        assertEquals(4, insertTuples.get(0).size());
        assertEquals("J. K. Rowling", insertTuples.get(0).getField(2).get().getString());

        //repeat insert same data
        assertDoesNotThrow(() -> testSpace.replace(tarantoolTuple).get());

        ImmutableArrayValue newValues = ValueFactory.newArray(
                new ImmutableLongValueImpl(200L),
                new ImmutableStringValueImpl("Jane Eyre"),
                new ImmutableStringValueImpl("Charlotte Brontë"),
                new ImmutableLongValueImpl(1847L)
        );
        TarantoolTuple newTupleWithSameId = new TarantoolTupleImpl(newValues, mapperFactory.defaultComplexTypesMapper());

        testSpace.replace(newTupleWithSameId);

        //select request
        TarantoolIndexQuery query = new TarantoolIndexQuery();
        query.withKeyValues(Collections.singletonList(200));
        TarantoolResult<TarantoolTuple> selectAfterReplaceResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        assertEquals(1, selectAfterReplaceResult.size());

        Optional<TarantoolTuple> value = selectAfterReplaceResult.stream().findFirst();
        assertTrue(value.isPresent());

        assertEquals(200, value.get().getField(0).get().getInteger());
        assertEquals("Jane Eyre", value.get().getField(1).get().getString());
        assertEquals("Charlotte Brontë", value.get().getField(2).get().getString());
        assertEquals(1847, value.get().getField(3).get().getInteger());
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
        assertEquals(deletedId, deleteRequestResult.get(0).getField(0).get().getInteger());

        //select after delete request
        TarantoolResult<TarantoolTuple> selectAfterDeleteResult = testSpace.select(query, new TarantoolSelectOptions()).get();

        Optional<TarantoolTuple> deletedValue = selectAfterDeleteResult.stream()
                .filter((v) -> v.getField(0).get().getInteger() == deletedId).findFirst();
        assertEquals(0, selectAfterDeleteResult.size());
        assertFalse(deletedValue.isPresent());
    }
}
