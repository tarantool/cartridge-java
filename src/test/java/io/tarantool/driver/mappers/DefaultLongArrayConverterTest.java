package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultLongArrayToArrayValueConverter;
import io.tarantool.driver.mappers.converters.value.DefaultArrayValueToLongArrayConverter;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableArrayValue;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultLongArrayConverterTest {

    @Test
    void should_fromValue_returnLongArrayValue() {
        DefaultArrayValueToLongArrayConverter converter = new DefaultArrayValueToLongArrayConverter();

        ImmutableArrayValue arrayValue = ValueFactory.newArray(new ImmutableLongValueImpl(1L), new ImmutableLongValueImpl(2L), new ImmutableLongValueImpl(3L));
        assertTrue(converter.canConvertValue(arrayValue));

        long[] longs = converter.fromValue(arrayValue);
        assertEquals(3, longs.length);
        assertArrayEquals(new long[]{1L, 2L, 3L}, longs);
    }

    @Test
    void should_returnFalse_ifArrayValueIsString() {
        DefaultArrayValueToLongArrayConverter converter = new DefaultArrayValueToLongArrayConverter();

        ImmutableArrayValue arrayValue = ValueFactory.newArray(ValueFactory.newString("notLong"));

        assertFalse(converter.canConvertValue(arrayValue));
    }

    @Test
    void should_throwException_ifArrayValueContainsNotFirstString() {
        DefaultArrayValueToLongArrayConverter converter = new DefaultArrayValueToLongArrayConverter();

        ImmutableArrayValue arrayValue = ValueFactory.newArray(new ImmutableLongValueImpl(2L), new ImmutableStringValueImpl("string"), new ImmutableLongValueImpl(3L));
        assertTrue(converter.canConvertValue(arrayValue));

        assertThrows(MessageTypeCastException.class, () -> converter.fromValue(arrayValue));
    }

    @Test
    void should_fromLongArray_returnArrayValue() {
        DefaultLongArrayToArrayValueConverter converter = new DefaultLongArrayToArrayValueConverter();

        ArrayValue result = converter.toValue(new long[]{1L, 2L, 3L});
        assertEquals(ValueFactory.newArray(new ImmutableLongValueImpl(1L), new ImmutableLongValueImpl(2L), new ImmutableLongValueImpl(3L)), result);
    }
}
