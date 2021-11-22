package io.tarantool.driver.api.tuple.operations;


/**
 * Represents bitwise OR operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationBitwiseOr extends TupleUpdateOperation {

    public TupleOperationBitwiseOr(int fieldIndex, long value) {
        super(TarantoolUpdateOperationType.BITWISEOR, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationBitwiseOr(String fieldName, long value) {
        super(TarantoolUpdateOperationType.BITWISEOR, fieldName, value);
        checkValue(value);
    }

    private TupleOperationBitwiseOr(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                    String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationBitwiseOr(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                true
        );
    }

    private void checkValue(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Bitwise operations can be performed only with values >= 0");
        }
    }

    @Override
    public TupleOperation cloneWithIndex(int fieldMetadataIndex) {
        return new TupleOperationBitwiseOr(
                this.getOperationType(),
                fieldMetadataIndex,
                this.getFieldName(),
                this.getValue(),
                this.isProxyOperation()
        );
    }
}
