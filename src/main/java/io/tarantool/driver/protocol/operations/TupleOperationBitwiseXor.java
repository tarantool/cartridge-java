package io.tarantool.driver.protocol.operations;


/**
 * Represents bitwise XOR operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationBitwiseXor extends TupleUpdateOperation {

    public TupleOperationBitwiseXor(int fieldIndex, long value) {
        super(TarantoolUpdateOperationType.BITWISEXOR, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationBitwiseXor(String fieldName, long value) {
        super(TarantoolUpdateOperationType.BITWISEXOR, fieldName, value);
        checkValue(value);
    }

    private void checkValue(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Bitwise operations can be performed only with values >= 0");
        }
    }
}
