package io.tarantool.driver.api.tuple.operations;


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

    private TupleOperationSet(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                              String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationSet(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                true
        );
    }
}
