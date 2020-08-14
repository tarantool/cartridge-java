package io.tarantool.driver.protocol.operations;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;
import org.springframework.util.StringUtils;

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
     * @param fieldIndex field number starting with 0
     * @param value operation value
     */
    public TupleUpdateOperation(TarantoolOperationType operationType, int fieldIndex, Object value) {

        checkValue(operationType, value);
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
        if (StringUtils.isEmpty(fieldName)) {
            throw new IllegalArgumentException("Field name must be not empty");
        }
        checkValue(operationType, value);
        this.operationType = operationType;
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(
                Arrays.asList(getOperationType().toString(), getFieldIndex(), getValue()));
    }

    @Override
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

    private void checkValue(TarantoolOperationType operationType, Object value) {

        switch (operationType) {
            case BITWISEXOR:
            case BITWISEOR:
            case BITWISEAND:
                if (value == null || (int) value < 0) {
                    throw new IllegalArgumentException("Bitwise operations can perform only with values >= 0");
                }
                break;
            case DELETE:
                if (value == null || (int) value <= 0) {
                    throw new IllegalArgumentException("The number of fields to remove must be greater than zero");
                }
                break;
        }
    }
}
