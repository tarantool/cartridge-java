package io.tarantool.driver.protocol.operations;


import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Create list of {@link TupleOperation} for update and upsert requests
 *
 * @author Sergey Volgin
 */
public final class TupleOperations {

    private List<TupleOperation> operations;

    private TupleOperations(TupleOperation operation) {
        this.operations = new ArrayList<TupleOperation>() {{
            add(operation);
        }};
    }

    /**
     * Add operation to the list
     *
     * @param operation for field
     * @return this
     */
    public TupleOperations addOperation(TupleOperation operation) {
        Integer filedIndex = operation.getFieldIndex();
        String fieldName = operation.getFieldName();

        Optional<TupleOperation> existField;
        if (filedIndex != null) {
            existField = this.operations.stream().filter(op -> filedIndex.equals(op.getFieldIndex())).findFirst();
        } else {
            existField = this.operations.stream().filter(op -> fieldName.equals(op.getFieldName())).findFirst();
        }

        if (existField.isPresent()) {
            throw new TarantoolSpaceOperationException("Double update of the same field (%s)",
                    filedIndex != null ? filedIndex : fieldName);
        }

        this.operations.add(operation);
        return this;
    }

    /**
     * Get list of operations
     *
     * @return list of operations
     */
    public List<TupleOperation> asList() {
        return operations;
    }

    /**
     * Adds the specified value to the field value
     *
     * @param fieldIndex field number starting with 0
     * @param value increment
     * @return new instance
     */
    public static TupleOperations add(int fieldIndex, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldIndex, value));
    }

    /**
     * Adds the specified value to the field value
     *
     * @param fieldIndex field number starting with 0
     * @param value increment
     * @return this
     */
    public TupleOperations andAdd(int fieldIndex, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldIndex, value));
    }

    /**
     * Adds the specified value to the field value
     *
     * @param fieldName field name
     * @param value increment
     * @return new instance
     */
    public static TupleOperations add(String fieldName, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldName, value));
    }

    /**
     * Adds the specified value to the field value
     *
     * @param fieldName field name
     * @param value increment
     * @return this
     */
    public TupleOperations andAdd(String fieldName, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldName, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseAnd(int fieldIndex, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldIndex, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseAnd(int fieldIndex, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldIndex, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseAnd(String fieldName, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldName, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseAnd(String fieldName, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldName, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseOr(int fieldIndex, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldIndex, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseOr(int fieldIndex, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldIndex, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseOr(String fieldName, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldName, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseOr(String fieldName, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldName, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseXor(int fieldIndex, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldIndex, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldIndex field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseXor(int fieldIndex, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldIndex, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseXor(String fieldName, int value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldName, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseXor(String fieldName, int value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldName, value));
    }

    /**
     * Remove field value
     *
     * @param fieldIndex start field number starting with 0
     * @param fieldsCount the number of fields to remove
     * @return new instance
     */
    public static TupleOperations delete(int fieldIndex, int fieldsCount) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldIndex, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldIndex start field number starting with 0
     * @param fieldsCount the number of fields to remove
     * @return this
     */
    public TupleOperations andDelete(int fieldIndex, int fieldsCount) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldIndex, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldName field name
     * @return new instance
     */
    public static TupleOperations delete(String fieldName, int fieldsCount) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldName, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldName field name
     * @return this
     */
    public TupleOperations andDelete(String fieldName, int fieldsCount) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldName, fieldsCount));
    }

    /**
     * Insert field value
     *
     * @param fieldIndex field number starting with 0
     * @return new instance
     */
    public static TupleOperations insert(int fieldIndex, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldIndex, value));
    }

    /**
     * Insert field value
     *
     * @param fieldIndex field number starting with 0
     * @return this
     */
    public TupleOperations andInsert(int fieldIndex, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldIndex, value));
    }

    /**
     * Insert field value
     *
     * @param fieldName field name
     * @return new instance
     */
    public static TupleOperations insert(String fieldName, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldName, value));
    }

    /**
     * Insert field value
     *
     * @param fieldName field name
     * @return this
     */
    public TupleOperations andInsert(String fieldName, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldName, value));
    }

    /**
     * Set field value
     *
     * @param fieldIndex field number starting with 0
     * @return new instance
     */
    public static TupleOperations set(int fieldIndex, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SET, fieldIndex, value));
    }

    /**
     * Set field value
     *
     * @param fieldIndex field number starting with 0
     * @return this
     */
    public TupleOperations andSet(int fieldIndex, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SET, fieldIndex, value));
    }

    /**
     * Set field value
     *
     * @param fieldName field name
     * @return new instance
     */
    public static TupleOperations set(String fieldName, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SET, fieldName, value));
    }

    /**
     * Set field value
     *
     * @param fieldName field name
     * @return this
     */
    public TupleOperations andSet(String fieldName, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SET, fieldName, value));
    }

    /**
     * Replace substring
     *
     * @param fieldIndex field number starting with 0
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return new instance
     */
    public static TupleOperations splice(int fieldIndex, int position, int offset, String replacement) {
        return new TupleOperations(new TupleSpliceOperation(fieldIndex, position, offset, replacement));
    }

    /**
     * Replace substring
     *
     * @param fieldIndex field number starting with 0
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return this
     */
    public TupleOperations andSplice(int fieldIndex, int position, int offset, String replacement) {
        return addOperation(new TupleSpliceOperation(fieldIndex, position, offset, replacement));
    }

    /**
     * Replace substring
     *
     * @param fieldName field name
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return new instance
     */
    public static TupleOperations splice(String fieldName, int position, int offset, String replacement) {
        return new TupleOperations(new TupleSpliceOperation(fieldName, position, offset, replacement));
    }

    /**
     * Replace substring
     *
     * @param fieldName field name
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return this
     */
    public TupleOperations andSplice(String fieldName, int position, int offset, String replacement) {
        return addOperation(new TupleSpliceOperation(fieldName, position, offset, replacement));
    }

    /**
     * Subtracts the specified value to the field value
     *
     * @param fieldIndex field number starting with 0
     * @param value increment
     * @return this
     */
    public static TupleOperations subtract(int fieldIndex, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldIndex, value));
    }

    /**
     * Subtracts the specified value to the field value
     *
     * @param fieldIndex field number starting with 0
     * @param value increment
     * @return this
     */
    public TupleOperations andSubtract(int fieldIndex, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldIndex, value));
    }

    /**
     * Subtracts the specified value to the field value
     *
     * @param fieldName field name
     * @param value increment
     * @return this
     */
    public static TupleOperations subtract(String fieldName, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldName, value));
    }

    /**
     * Subtracts the specified value to the field value
     *
     * @param fieldName field name
     * @param value increment
     * @return this
     */
    public TupleOperations andSubtract(String fieldName, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldName, value));
    }
}
