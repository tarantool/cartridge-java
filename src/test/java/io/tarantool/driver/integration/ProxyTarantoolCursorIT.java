package io.tarantool.driver.integration;

import io.tarantool.driver.ProxyTarantoolClient;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.cursor.TarantoolCursorOptions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class ProxyTarantoolCursorIT extends SharedCartridgeContainer {

    private static final String CURSOR_SPACE_NAME = "cursor_test_space";

    protected static ProxyTarantoolClient client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() throws ExecutionException, InterruptedException {
        client = createClusterClient();
        insertCursorTestData();
    }

    private static void insertCursorTestData() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);

        for (int i = 1; i <= 100; i++) {
            //insert new data
            List<Object> values = Arrays.asList(i, i + "abc", i * 10);
            TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
            testSpace.insert(tarantoolTuple).get();
        }
    }

    @Test
    public void getOneTuple() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);
        Conditions conditions = Conditions.equals("id", Collections.singletonList(1));

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions());

        assertTrue(cursor.hasNext());
        TarantoolTuple tuple = cursor.next();
        assertFalse(cursor.hasNext());

        assertEquals(1, tuple.getInteger(0));
        assertEquals("1abc", tuple.getString(1));
        assertEquals(10, tuple.getInteger(2));
        assertTrue(tuple.getInteger(3) > 0); //bucket_id

        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void getTuples_withLimitAndCondition() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);

        Conditions conditions = Conditions
                .greaterOrEquals("id", Collections.singletonList(12))
                .withLimit(13);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(13, countTotal);
        assertEquals(Arrays.asList(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24), tupleIds);
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void getTuples_withLimitAndConditionDescending() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);

        Conditions conditions = Conditions
                .lessOrEquals("id", Collections.singletonList(55))
                .withLimit(13);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(13, countTotal);
        assertEquals(Arrays.asList(55, 54, 53, 52, 51, 50, 49, 48, 47, 46, 45, 44, 43), tupleIds);
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void countAll_batchByOneElement() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        int countTotal = 0;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);

        cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(1));
        assertTrue(cursor.hasNext());

        countTotal = 0;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_smallBatch() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(10));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_BatchEqualCount() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(100));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_largeBatch() {
        TarantoolSpaceOperations testSpace = client.space(CURSOR_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(1000));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }
}
