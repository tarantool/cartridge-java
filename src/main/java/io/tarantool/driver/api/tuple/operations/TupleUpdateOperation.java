package io.tarantool.driver.api.tuple.operations;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Objects;

/**
 * An operation specifies one value.
 *
 * @see <a href="https://www.tarantool.io/en/doc/2.5/reference/reference_lua/box_space/#box-space-update">
 *     https://www.tarantool.io/en/doc/2.5/reference/reference_lua/box_space/#box-space-update</a>
 *
 * @author Sergey Volgin
 */
abstract class TupleUpdateOperation implements TupleOperation {

    protected final boolean isProxyOperation;
    protected final TarantoolUpdateOperationType operationType;
    protected Integer fieldIndex;
    protected String fieldName;
    protected final Object value;

    /**
     * Create instance
     *
     * @param operationType operation type
     * @param fieldIndex field index starting with 0
     * @param value operation value
     */
    TupleUpdateOperation(TarantoolUpdateOperationType operationType, int fieldIndex, Object value) {
        this(operationType, fieldIndex, null, value, false);
    }

    /**
     * Create instance
     *
     * @param operationType operation type
     * @param fieldName field name
     * @param value operation value
     */
    TupleUpdateOperation(TarantoolUpdateOperationType operationType, String fieldName, Object value) {
        this(operationType, null, fieldName, value, false);
    }

    TupleUpdateOperation(TarantoolUpdateOperationType operationType,
                                   Integer fieldIndex,
                                   String fieldName,
                                   Object value,
                                   boolean isProxyOperation) {
        if (fieldIndex == null && StringUtils.isEmpty(fieldName)) {
            throw new IllegalArgumentException("Field name must be not empty");
        }
        this.isProxyOperation = isProxyOperation;
        this.operationType = operationType;
        this.fieldIndex = fieldIndex;
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(
                Arrays.asList(getOperationType().toString(), getFieldIndex(), getValue()));
    }

    @Override
    public TarantoolUpdateOperationType getOperationType() {
        return operationType;
    }

    @Override
    public Integer getFieldIndex() {
        return fieldIndex;
    }

    @Override
    public void setFieldIndex(Integer fieldIndex) {
        this.fieldIndex = fieldIndex;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public Boolean isProxyOperation() {
        return isProxyOperation;
    }

    /**
     * Convert field index to field number
     *
     * @return field number started from 1
     */
    protected Integer getFieldNumber() {
        if (this.getFieldIndex() == null) {
            return null;
        }
        return this.isProxyOperation() ? this.getFieldIndex() : this.getFieldIndex() + 1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleUpdateOperation that = (TupleUpdateOperation) o;
        return operationType == that.operationType &&
                Objects.equals(fieldIndex, that.fieldIndex) &&
                Objects.equals(fieldName, that.fieldName) &&
                value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operationType, fieldIndex, fieldName, value);
    }
}
