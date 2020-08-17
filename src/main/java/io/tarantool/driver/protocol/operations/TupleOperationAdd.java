package io.tarantool.driver.protocol.operations;


/**
 * Represents addition operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationAdd extends TupleUpdateOperation {

    public TupleOperationAdd(int fieldIndex, Number value) {
        super(TarantoolUpdateOperationType.ADD, fieldIndex, value);
    }

    public TupleOperationAdd(String fieldName, Number value) {
        super(TarantoolUpdateOperationType.ADD, fieldName, value);
    }
}
