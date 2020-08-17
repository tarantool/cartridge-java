package io.tarantool.driver.protocol.operations;


/**
 * Represents subtraction operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationSubtract extends TupleUpdateOperation {

    public TupleOperationSubtract(int fieldIndex, Number value) {
        super(TarantoolUpdateOperationType.SUBTRACT, fieldIndex, value);
    }

    public TupleOperationSubtract(String fieldName, Number value) {
        super(TarantoolUpdateOperationType.SUBTRACT, fieldName, value);
    }
}
