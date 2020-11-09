package io.tarantool.driver.api.tuple.operations;


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

    private TupleOperationSubtract(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                   String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationSubtract(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                true
        );
    }
}
