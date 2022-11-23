package io.tarantool.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * @author Oleg Kuznetsov
 */
public class TarantoolVersionTest {

    @Test
    public void test_should_createFromString() {
        assertDoesNotThrow(() -> TarantoolVersion.fromString("Tarantool 1.10"));
    }

    @Test
    public void test_should_ThrowException_ifVersionIsIncorrect() {
        assertThrows(InvalidVersionException.class, () -> TarantoolVersion.fromString("carantool 1.10"));
    }

    @Test
    public void test_should_beEquals_asStrings() throws InvalidVersionException {
        final String versionString = "Tarantool 1.10";
        assertEquals(versionString, TarantoolVersion.fromString(versionString).toString());
    }

    @Test
    public void test_should_beEquals() throws InvalidVersionException {
        final String versionString = "Tarantool 1.10";

        assertEquals(TarantoolVersion.fromString(versionString), TarantoolVersion.fromString(versionString));
        final TarantoolVersion expected = TarantoolVersion.fromString(versionString);
        assertEquals(expected, expected);
        assertNotEquals(expected, null);
    }

    @Test
    public void test_should_hashCodeIsCorrect() throws InvalidVersionException {
        final String versionString = "Tarantool 1.10";

        assertEquals(TarantoolVersion.fromString(versionString).hashCode(),
            TarantoolVersion.fromString(versionString).hashCode());
    }
}
