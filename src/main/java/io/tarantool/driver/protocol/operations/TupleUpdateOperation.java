package io.tarantool.driver.protocol.operations;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;

import java.util.Arrays;

/**
 * An operation specifies one value.
 *
 * @author Sergey Volgin
 */
public class TupleUpdateOperation implements TupleOperation {

    protected final TarantoolOperationType operationType;
    protected Integer fieldIndex;
    protected String fieldName;
    protected final Object value;

    /**
     * Create instance
     *
     * @param operationType operation type
     * @param fieldIndex field number starting with 1
     * @param value operation value
     */
    public TupleUpdateOperation(TarantoolOperationType operationType, int fieldIndex, Object value) {
        this.operationType = operationType;
        this.fieldIndex = fieldIndex;
        this.value = value;
    }

    /**
     * Create instance
     *
     * @param operationType operation type
     * @param fieldName field name
     * @param value operation value
     */
    public TupleUpdateOperation(TarantoolOperationType operationType, String fieldName, Object value) {
        this.operationType = operationType;
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(
                Arrays.asList(getOperationType().toString(), getFieldIndex(), getValue()));
    }

    public TarantoolOperationType getOperationType() {
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

    public Object getValue() {
        return value;
    }
}
