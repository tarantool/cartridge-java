package io.tarantool.driver.protocol.operations;


/**
 * Represents deletion operation on tuple
 *
 * @author Sergey Volgin
 */
public class TupleOperationDelete extends TupleUpdateOperation {

    public TupleOperationDelete(int fieldIndex, int value) {
        super(TarantoolUpdateOperationType.DELETE, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationDelete(String fieldName, int value) {
        super(TarantoolUpdateOperationType.DELETE, fieldName, value);
        checkValue(value);
    }

    private TupleOperationDelete(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                 String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationDelete(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                true
        );
    }

    private void checkValue(int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
        }
    }
}
