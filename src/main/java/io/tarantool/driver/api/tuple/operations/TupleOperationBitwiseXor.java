package io.tarantool.driver.api.tuple.operations;


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

    private TupleOperationBitwiseXor(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                     String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationBitwiseXor(
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
        return new TupleOperationBitwiseXor(
                this.getOperationType(),
                fieldIndex,
                this.getFieldName(),
                this.getValue(),
                this.isProxyOperation()
        );
    }
}
