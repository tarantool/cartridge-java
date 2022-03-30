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
        TarantoolNullField nullField = new TarantoolNullField();

        assertNotEquals(0, nullField.hashCode());
        assertEquals(nullField.hashCode(), nullField.hashCode());
    }

    @Test
    public void test_AddNullFieldsToHashSet_shouldCreateHashSetWithTwoElements() {
        // given
        TarantoolNullField nullField1 = new TarantoolNullField();
        TarantoolNullField nullField2 = new TarantoolNullField();

        // when
        HashSet<TarantoolNullField> fieldsSet = new HashSet<>();
        fieldsSet.add(nullField1);
        fieldsSet.add(nullField2);

        // then
        assertEquals(2, fieldsSet.size());
        assertTrue(fieldsSet.contains(nullField1));
        assertTrue(fieldsSet.contains(nullField2));
    }
}