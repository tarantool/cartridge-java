package io.tarantool.driver.api.tuple.operations;


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

    private TupleOperationAdd(
        TarantoolUpdateOperationType operationType, Integer fieldIndex,
        String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationAdd(
            this.getOperationType(),
            this.getFieldNumber(),
            this.getFieldName(),
            this.getValue(),
            true
        );
    }

    @Override
    public TupleOperation cloneWithIndex(int fieldMetadataIndex) {
        return new TupleOperationAdd(
            this.getOperationType(),
            fieldMetadataIndex,
            this.getFieldName(),
            this.getValue(),
            this.isProxyOperation()
        );
    }
}
