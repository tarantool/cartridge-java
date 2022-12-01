package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.core.metadata.TarantoolMetadata;
import io.tarantool.driver.core.metadata.TestMetadataProvider;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import io.tarantool.driver.protocol.TarantoolIndexQuery;
import io.tarantool.driver.protocol.TarantoolIndexQueryFactory;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Alexey Kuzin
 */
class ConditionsTest {

    private final TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());

    @Test
    public void testProxyQuery_OffsetNotSupported() {
        Conditions conditions = Conditions.offset(100);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toProxyQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Offset is not supported", ex.getMessage());
    }

    @Test
    public void testProxyQuery_AfterIsSupported() {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolTupleFactory factory = new DefaultTarantoolTupleFactory(defaultMapper);
        TarantoolTuple startTuple = factory.create(1, 2, 3);
        Conditions conditions = Conditions.any().startAfter(startTuple);

        assertEquals(startTuple, conditions.getStartTuple());
    }

    @Test
    public void testProxyQuery_FilteringByMultipleIndexesNotSupported() {
        Conditions conditions = Conditions
            .indexEquals(0, Collections.singletonList("abc"))
            .andIndexEquals(1, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toProxyQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Filtering by more than one index is not supported", ex.getMessage());
    }

    @Test
    public void testProxyQuery_IndexConditionsPrecedeFieldConditions() {
        ArrayList<Integer> startIndex = new ArrayList<>(Arrays.asList(1, 2, 3));
        ArrayList<Integer> endIndex = new ArrayList<>(Arrays.asList(4, 5, 6));
        Conditions conditions = Conditions
            .equals("third", 456)
            .andIndexGreaterOrEquals(0, startIndex)
            .andEquals(1, 123)
            .andIndexLessOrEquals("primary", endIndex);

        List<?> query = Arrays.asList(
            Arrays.asList(">=", "primary", Arrays.asList(1, 2, 3)),
            Arrays.asList("<=", "primary", Arrays.asList(4, 5, 6)),
            Arrays.asList("=", "third", 456),
            Arrays.asList("=", 1, 123)
        );

        assertEquals(query, conditions.toProxyQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testProxyQuery_FieldConditionsForSuitableIndexGoFirst() {
        Conditions conditions = Conditions
            .equals("fourth", 789)
            .andEquals("third", 456)
            .andEquals("first", 123);

        List<?> query = Arrays.asList(
            Arrays.asList("=", "first", 123),
            Arrays.asList("=", "third", 456),
            Arrays.asList("=", "fourth", 789)
        );

        assertEquals(query, conditions.toProxyQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_AfterNotSupported() {
        TarantoolTupleFactory factory = new DefaultTarantoolTupleFactory(
            DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper());
        Conditions conditions = Conditions.after(factory.create());

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("'startAfter' is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_MultipleConditionsPerOneIndexNotSupported() {
        Conditions conditions = Conditions
            .indexEquals(0, Collections.singletonList("abc"))
            .andIndexEquals(0, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Multiple conditions for one index are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_FilteringByMultipleIndexesNotSupported() {
        Conditions conditions = Conditions
            .indexEquals(0, Collections.singletonList("abc"))
            .andIndexEquals(1, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Filtering by more than one index is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_MultipleConditionsPerOneFieldNotSupported() {
        Conditions conditions = Conditions
            .equals(0, "abc")
            .andEquals(0, 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Multiple conditions for one field are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_FilteringByIndexAndFieldsNotSupported() {
        Conditions conditions = Conditions
            .indexEquals("primary", Collections.singletonList("abc"))
            .andEquals(0, 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Filtering simultaneously by index and fields is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongIndexName() {
        Conditions conditions = Conditions
            .indexEquals("kekeke", Collections.singletonList("abc"))
            .andEquals(0, 123);

        TarantoolIndexNotFoundException ex = assertThrows(TarantoolIndexNotFoundException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Index 'kekeke' is not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongIndexId() {
        Conditions conditions = Conditions
            .indexEquals(1000, Collections.singletonList("abc"))
            .andEquals(0, 123);

        TarantoolIndexNotFoundException ex = assertThrows(TarantoolIndexNotFoundException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Index with id 1000 is not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongFieldName() {
        Conditions conditions = Conditions
            .indexEquals("asecondary1", Collections.singletonList(123))
            .andEquals("wrong", 123);

        TarantoolFieldNotFoundException ex = assertThrows(TarantoolFieldNotFoundException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Field 'wrong' not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongFieldId() {
        Conditions conditions = Conditions
            .indexEquals("asecondary1", Collections.singletonList(123))
            .andEquals(5, 123);

        TarantoolFieldNotFoundException ex = assertThrows(TarantoolFieldNotFoundException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Field with id 5 not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_DifferentConditionsForIndexPartNotSupported() {
        Conditions conditions = Conditions.any()
            .andGreaterThan("second", 123)
            .andLessThan("third", 456);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("Different conditions for index parts are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_defaultQuery() {
        Conditions conditions = Conditions.any();

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary();

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_primaryDescending() {
        Conditions conditions = Conditions.descending()
            .andIndexLessOrEquals("primary", Collections.singletonList("abc"));

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary()
            .withIteratorType(TarantoolIteratorType.ITER_GE)
            .withKeyValues(Collections.singletonList("abc"));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_autoselectPrimaryIndex() {
        Conditions conditions = Conditions.greaterOrEquals("first", "abc");

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary()
            .withIteratorType(TarantoolIteratorType.ITER_GE)
            .withKeyValues(Collections.singletonList("abc"));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_autoselectSmallestIndex() {
        Conditions conditions = Conditions.lessOrEquals("second", 456);

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).byName(512, "asecondary")
            .withIteratorType(TarantoolIteratorType.ITER_LE)
            .withKeyValues(Collections.singletonList(456));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_autoselectSmallestIndex2() {
        Conditions conditions = Conditions
            .lessThan("fourth", 456)
            .andLessThan("second", 123);

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).byName(512, "secondary2")
            .withIteratorType(TarantoolIteratorType.ITER_LT)
            .withKeyValues(Arrays.asList(123, 456));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_suitableSecondaryIndexByOneField() {
        Conditions conditions = Conditions.greaterThan("fourth", 456);

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).byName(512, "secondary2")
            .withIteratorType(TarantoolIteratorType.ITER_GT)
            .withKeyValues(Arrays.asList(null, 456));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));
    }

    @Test
    public void testIndexQuery_noSuitableIndex() {
        Conditions conditions = Conditions
            .greaterOrEquals("first", "abc")
            .andGreaterOrEquals("second", 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
            () -> conditions.toIndexQuery(testOperations, testOperations.getSpaceByName("test").get()));

        assertEquals("No indexes that fit the passed fields are found", ex.getMessage());
    }

    @Test
    public void testSerialize() throws IOException, ClassNotFoundException {
        MessagePackMapper defaultMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
        TarantoolTupleFactory factory = new DefaultTarantoolTupleFactory(defaultMapper);
        TarantoolMetadata testOperations = new TarantoolMetadata(new TestMetadataProvider());
        TarantoolSpaceMetadata spaceMetadata = testOperations.getSpaceByName("test").get();
        TarantoolTuple startTuple = factory.create(1, 2, 3);
        Conditions conditions = Conditions
            .greaterOrEquals("first", "abc")
            .andGreaterOrEquals("second", 123)
            .startAfter(startTuple)
            .withLimit(100)
            .withDescending();

        byte[] buffer;
        int len = 4096;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(len);
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(conditions);
            oos.flush();
            buffer = bos.toByteArray();
        }
        Conditions serializedConditions;
        try (ByteArrayInputStream bis = new ByteArrayInputStream(buffer);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            serializedConditions = (Conditions) ois.readObject();
        }

        assertEquals(conditions.getOffset(), serializedConditions.getOffset());
        assertEquals(conditions.getLimit(), serializedConditions.getLimit());
        assertEquals(conditions.isDescending(), serializedConditions.isDescending());

        TarantoolTuple serializedTuple = (TarantoolTuple) serializedConditions.getStartTuple();
        assertEquals(startTuple.size(), serializedTuple.size());
        assertEquals(startTuple.getInteger(0), serializedTuple.getInteger(0));
        assertEquals(startTuple.getInteger(1), serializedTuple.getInteger(1));
        assertEquals(startTuple.getInteger(2), serializedTuple.getInteger(2));

        assertEquals(conditions.toProxyQuery(testOperations, spaceMetadata),
            serializedConditions.toProxyQuery(testOperations, spaceMetadata));
    }
}
