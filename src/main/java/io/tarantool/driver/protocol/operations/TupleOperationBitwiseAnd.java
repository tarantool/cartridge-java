package io.tarantool.driver.protocol.operations;


/**
 * Represents bitwise AND operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationBitwiseAnd extends TupleUpdateOperation {

    public TupleOperationBitwiseAnd(int fieldIndex, long value) {
        super(TarantoolUpdateOperationType.BITWISEAND, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationBitwiseAnd(String fieldName, long value) {
        super(TarantoolUpdateOperationType.BITWISEAND, fieldName, value);
        checkValue(value);
    }

    private void checkValue(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Bitwise operations can be performed only with values >= 0");
        }
    }
}
