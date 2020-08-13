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
     * @param fieldNumber field number starting with 0
     * @param value increment
     * @return new instance
     */
    public static TupleOperations add(int fieldNumber, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldNumber, value));
    }

    /**
     * Adds the specified value to the field value
     *
     * @param fieldNumber field number starting with 0
     * @param value increment
     * @return this
     */
    public TupleOperations andAdd(int fieldNumber, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.ADD, fieldNumber, value));
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
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseAnd(int fieldNumber, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldNumber, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseAnd(int fieldNumber, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldNumber, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseAnd(String fieldName, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldName, value));
    }

    /**
     * Bitwise AND(&) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseAnd(String fieldName, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEAND, fieldName, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseOr(int fieldNumber, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldNumber, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseOr(int fieldNumber, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldNumber, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseOr(String fieldName, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldName, value));
    }

    /**
     * Bitwise OR(|) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseOr(String fieldName, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEOR, fieldName, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseXor(int fieldNumber, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldNumber, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldNumber field number starting with 0
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseXor(int fieldNumber, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldNumber, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldName field name
     * @param value value
     * @return new instance
     */
    public static TupleOperations bitwiseXor(String fieldName, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldName, value));
    }

    /**
     * Bitwise XOR(^) operation
     *
     * @param fieldName field name
     * @param value value
     * @return this
     */
    public TupleOperations andBitwiseXor(String fieldName, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.BITWISEXOR, fieldName, value));
    }

    /**
     * Remove field value
     *
     * @param fieldNumber start field number starting with 0
     * @param fieldsCount the number of fields to remove
     * @return new instance
     */
    public static TupleOperations delete(int fieldNumber, int fieldsCount) {
        if (fieldsCount <= 0) {
            throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
        }
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldNumber, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldNumber start field number starting with 0
     * @param fieldsCount the number of fields to remove
     * @return this
     */
    public TupleOperations andDelete(int fieldNumber, int fieldsCount) {
        if (fieldsCount <= 0) {
            throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
        }
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldNumber, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldName field name
     * @return new instance
     */
    public static TupleOperations delete(String fieldName, int fieldsCount) {
        if (fieldsCount <= 0) {
            throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
        }
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldName, fieldsCount));
    }

    /**
     * Remove field value
     *
     * @param fieldName field name
     * @return this
     */
    public TupleOperations andDelete(String fieldName, int fieldsCount) {
        if (fieldsCount <= 0) {
            throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
        }
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.DELETE, fieldName, fieldsCount));
    }

    /**
     * Insert field value
     *
     * @param fieldNumber field number starting with 0
     * @return new instance
     */
    public static TupleOperations insert(int fieldNumber, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldNumber, value));
    }

    /**
     * Insert field value
     *
     * @param fieldNumber field number starting with 0
     * @return this
     */
    public TupleOperations andInsert(int fieldNumber, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.INSERT, fieldNumber, value));
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
     * @param fieldNumber field number starting with 0
     * @return new instance
     */
    public static TupleOperations set(int fieldNumber, Object value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SET, fieldNumber, value));
    }

    /**
     * Set field value
     *
     * @param fieldNumber field number starting with 0
     * @return this
     */
    public TupleOperations andSet(int fieldNumber, Object value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SET, fieldNumber, value));
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
     * @param fieldNumber field number starting with 0
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return new instance
     */
    public static TupleOperations splice(int fieldNumber, int position, int offset, String replacement) {
        return new TupleOperations(new TupleSpliceOperation(fieldNumber, position, offset, replacement));
    }

    /**
     * Replace substring
     *
     * @param fieldNumber field number starting with 0
     * @param position the start'th position
     * @param offset length of substring
     * @param replacement new value
     * @return this
     */
    public TupleOperations andSplice(int fieldNumber, int position, int offset, String replacement) {
        return addOperation(new TupleSpliceOperation(fieldNumber, position, offset, replacement));
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
     * @param fieldNumber field number starting with 0
     * @param value increment
     * @return this
     */
    public static TupleOperations subtract(int fieldNumber, Number value) {
        return new TupleOperations(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldNumber, value));
    }

    /**
     * Subtracts the specified value to the field value
     *
     * @param fieldNumber field number starting with 0
     * @param value increment
     * @return this
     */
    public TupleOperations andSubtract(int fieldNumber, Number value) {
        return addOperation(new TupleUpdateOperation(TarantoolOperationType.SUBTRACT, fieldNumber, value));
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
