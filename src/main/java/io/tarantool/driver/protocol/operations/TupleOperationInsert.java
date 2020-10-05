package io.tarantool.driver.protocol.operations;


/**
 * Represents insertion of a new field operation on tuple
 *
 * @author Sergey Volgin
 */
public class TupleOperationInsert extends TupleUpdateOperation {

    public TupleOperationInsert(int fieldIndex, Object value) {
        super(TarantoolUpdateOperationType.INSERT, fieldIndex, value);
    }

    public TupleOperationInsert(String fieldName, Object value) {
        super(TarantoolUpdateOperationType.INSERT, fieldName, value);
    }

    private TupleOperationInsert(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                 String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationInsert(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                true
        );
    }
}
