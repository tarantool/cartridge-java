package io.tarantool.driver.protocol.operations;


/**
 * Represents assignment operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationSet extends TupleUpdateOperation {

    public TupleOperationSet(int fieldIndex, Object value) {
        super(TarantoolUpdateOperationType.SET, fieldIndex, value);
    }

    public TupleOperationSet(String fieldName, Object value) {
        super(TarantoolUpdateOperationType.SET, fieldName, value);
    }
}
