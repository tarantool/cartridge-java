package io.tarantool.driver.protocol.operations;


/**
 * Represents bitwise OR operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationBitwiseOr extends TupleUpdateOperation {

    public TupleOperationBitwiseOr(int fieldIndex, long value) {
        super(TarantoolUpdateOperationType.BITWISEOR, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationBitwiseOr(String fieldName, long value) {
        super(TarantoolUpdateOperationType.BITWISEOR, fieldName, value);
        checkValue(value);
    }

    private void checkValue(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Bitwise operations can be performed only with values >= 0");
        }
    }
}
