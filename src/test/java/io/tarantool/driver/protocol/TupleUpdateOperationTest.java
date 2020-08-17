package io.tarantool.driver.protocol;


import io.tarantool.driver.protocol.operations.TupleOperationBitwiseAnd;
import io.tarantool.driver.protocol.operations.TupleOperationBitwiseOr;
import io.tarantool.driver.protocol.operations.TupleOperationBitwiseXor;
import io.tarantool.driver.protocol.operations.TupleOperationDelete;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TupleUpdateOperationTest {

    @Test
    public void checkValueTest() {
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationDelete(0, -1));
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationDelete(0, 0));

        assertDoesNotThrow(() -> new TupleOperationDelete(0, 1));
        assertDoesNotThrow(() -> new TupleOperationDelete(0, 15));

        assertThrows(IllegalArgumentException.class, () -> new TupleOperationBitwiseAnd(0, -1));
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationBitwiseOr(0, -3));
        assertThrows(IllegalArgumentException.class, () -> new TupleOperationBitwiseXor(0, -2));

        assertDoesNotThrow(() -> new TupleOperationBitwiseAnd(0, 5));
        assertDoesNotThrow(() -> new TupleOperationBitwiseOr(0, 10));
        assertDoesNotThrow(() -> new TupleOperationBitwiseXor(0, 15));
    }
}
