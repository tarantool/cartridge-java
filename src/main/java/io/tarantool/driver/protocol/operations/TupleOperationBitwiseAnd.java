package io.tarantool.driver.protocol.operations;


/**
 * Represents bitwise AND operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleOperationBitwiseAnd extends TupleUpdateOperation {

    public TupleOperationBitwiseAnd(int fieldIndex, long value) {
        super(TarantoolUpdateOperationType.BITWISEAND, fieldIndex, value);
        checkValue(value);
    }

    public TupleOperationBitwiseAnd(String fieldName, long value) {
        super(TarantoolUpdateOperationType.BITWISEAND, fieldName, value);
        checkValue(value);
    }

    private TupleOperationBitwiseAnd(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                     String fieldName, Object value, boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleOperationBitwiseAnd(
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
}
