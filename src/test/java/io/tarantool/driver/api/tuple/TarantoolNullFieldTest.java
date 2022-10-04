package io.tarantool.driver.api.tuple;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Dmitry Kasimovskiy
 */
public class TarantoolNullFieldTest {

    @Test
    public void test_hashCode_shouldReturnHashCode() {
        TarantoolNullField nullField = TarantoolNullField.EMPTY;

        assertNotEquals(0, nullField.hashCode());
        assertEquals(nullField.hashCode(), nullField.hashCode());
    }

    @Test
    public void test_toString_shouldReturnString() {
        // given
        TarantoolNullField nullField = TarantoolNullField.EMPTY;

        // when
        String str = nullField.toString();

        // then
        assertNotNull(str);
        assertFalse(str.isEmpty());
        assertEquals(str, nullField.toString());
    }

    @Test
    public void test_equals_shouldReturnTrue() {
        // given
        TarantoolNullField nullField1 = TarantoolNullField.EMPTY;
        TarantoolNullField nullField2 = TarantoolNullField.EMPTY;

        // then
        assertEquals(nullField1, nullField1);
        assertEquals(nullField1, nullField2);
    }

    @Test
    public void test_equals_shouldReturnFalse() {
        // given
        TarantoolNullField nullField = TarantoolNullField.EMPTY;
        Object dummyObject = new Object() { };

        // then
        assertNotEquals(nullField, null);
        assertNotEquals(nullField, dummyObject);
    }

    @Test
    public void test_AddNullFieldsToHashSet_shouldCreateHashSetWithOneElements() {
        // given
        TarantoolNullField nullField1 = TarantoolNullField.EMPTY;
        TarantoolNullField nullField2 = TarantoolNullField.EMPTY;

        // when
        HashSet<TarantoolNullField> fieldsSet = new HashSet<>();
        fieldsSet.add(nullField1);
        fieldsSet.add(nullField2);

        // then
        assertEquals(1, fieldsSet.size());
        assertTrue(fieldsSet.contains(nullField1));
        assertTrue(fieldsSet.contains(nullField2));
    }
}
