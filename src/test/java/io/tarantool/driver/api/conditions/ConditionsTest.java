package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.TarantoolIndexQueryFactory;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolFieldNotFoundException;
import io.tarantool.driver.exceptions.TarantoolIndexNotFoundException;
import io.tarantool.driver.metadata.TestMetadata;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author Alexey Kuzin
 */
class ConditionsTest {

    private final TestMetadata testOperations = new TestMetadata();

    @Test
    public void testProxyQuery_OffsetNotSupported() {
        Conditions conditions = Conditions.offset(100);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toProxyQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Offset is not supported", ex.getMessage());
    }

    @Test
    public void testProxyQuery_FilteringByMultipleIndexesNotSupported() {
        Conditions conditions = Conditions
                .indexEquals(0, Collections.singletonList("abc"))
                .andIndexEquals(1, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toProxyQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Filtering by more than one index is not supported", ex.getMessage());
    }

    @Test
    public void testProxyQuery_IndexConditionsPrecedeFieldConditions() {
        Conditions conditions = Conditions
                .equals("third", 456)
                .andIndexGreaterOrEquals(0, Collections.singletonList("abc"))
                .andEquals(1, 123)
                .andIndexLessOrEquals("primary", Collections.singletonList("def"));

        List<?> query = Arrays.asList(
                Arrays.asList(">=", "primary", Collections.singletonList("abc")),
                Arrays.asList("<=", "primary", Collections.singletonList("def")),
                Arrays.asList("=", "third", 456),
                Arrays.asList("=", 1, 123)
        );

        assertEquals(query, conditions.toProxyQuery(testOperations, testOperations.getTestSpaceMetadata()));
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

        assertEquals(query, conditions.toProxyQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_AfterNotSupported() {
        Conditions conditions = Conditions.after(Collections.emptyList());

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("'startAfter' is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_MultipleConditionsPerOneIndexNotSupported() {
        Conditions conditions = Conditions
                .indexEquals(0, Collections.singletonList("abc"))
                .andIndexEquals(0, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Multiple conditions for one index are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_FilteringByMultipleIndexesNotSupported() {
        Conditions conditions = Conditions
                .indexEquals(0, Collections.singletonList("abc"))
                .andIndexEquals(1, Collections.singletonList(123));

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Filtering by more than one index is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_MultipleConditionsPerOneFieldNotSupported() {
        Conditions conditions = Conditions
                .equals(0, "abc")
                .andEquals(0, 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Multiple conditions for one field are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_FilteringByIndexAndFieldsNotSupported() {
        Conditions conditions = Conditions
                .indexEquals("primary", Collections.singletonList("abc"))
                .andEquals(0, 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Filtering simultaneously by index and fields is not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongIndexName() {
        Conditions conditions = Conditions
                .indexEquals("kekeke", Collections.singletonList("abc"))
                .andEquals(0, 123);

        TarantoolIndexNotFoundException ex = assertThrows(TarantoolIndexNotFoundException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Index 'kekeke' is not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongIndexId() {
        Conditions conditions = Conditions
                .indexEquals(1000, Collections.singletonList("abc"))
                .andEquals(0, 123);

        TarantoolIndexNotFoundException ex = assertThrows(TarantoolIndexNotFoundException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Index with id 1000 is not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongFieldName() {
        Conditions conditions = Conditions
                .indexEquals("secondary1", Collections.singletonList(123))
                .andEquals("wrong", 123);

        TarantoolFieldNotFoundException ex = assertThrows(TarantoolFieldNotFoundException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Field 'wrong' not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_WrongFieldId() {
        Conditions conditions = Conditions
                .indexEquals("secondary1", Collections.singletonList(123))
                .andEquals(5, 123);

        TarantoolFieldNotFoundException ex = assertThrows(TarantoolFieldNotFoundException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Field with id 5 not found in space test", ex.getMessage());
    }

    @Test
    public void testIndexQuery_DifferentConditionsForIndexPartNotSupported() {
        Conditions conditions = Conditions.any()
                .andGreaterThan("second", 123)
                .andLessThan("third", 456);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("Different conditions for index parts are not supported", ex.getMessage());
    }

    @Test
    public void testIndexQuery_defaultQuery() {
        Conditions conditions = Conditions.any();

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary()
                .withKeyValues(Collections.singletonList(null));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_primaryDescending() {
        Conditions conditions = Conditions.descending()
                .andIndexLessOrEquals("primary", Collections.singletonList("abc"));

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary()
                .withIteratorType(TarantoolIteratorType.ITER_GE)
                .withKeyValues(Collections.singletonList("abc"));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_autoselectPrimaryIndex() {
        Conditions conditions = Conditions.greaterOrEquals("first", "abc");

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).primary()
                .withIteratorType(TarantoolIteratorType.ITER_GE)
                .withKeyValues(Collections.singletonList("abc"));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_autoselectSmallestIndex() {
        Conditions conditions = Conditions
                .lessThan("fourth", 456)
                .andLessThan("second", 123);

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).byName(512, "secondary2")
                .withIteratorType(TarantoolIteratorType.ITER_LT)
                .withKeyValues(Arrays.asList(123, 456));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_suitableSecondaryIndexByOneField() {
        Conditions conditions = Conditions.greaterThan("fourth", 456);

        TarantoolIndexQuery query = new TarantoolIndexQueryFactory(testOperations).byName(512, "secondary2")
                .withIteratorType(TarantoolIteratorType.ITER_GT)
                .withKeyValues(Arrays.asList(null, 456));

        assertEquals(query, conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));
    }

    @Test
    public void testIndexQuery_noSuitableIndex() {
        Conditions conditions = Conditions
                .greaterOrEquals("first", "abc")
                .andGreaterOrEquals("second", 123);

        TarantoolClientException ex = assertThrows(TarantoolClientException.class,
                () -> conditions.toIndexQuery(testOperations, testOperations.getTestSpaceMetadata()));

        assertEquals("No indexes that fit the passed fields are found", ex.getMessage());
    }
}