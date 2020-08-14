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
    protected Integer fieldNumber;
    protected String fieldName;
    protected final Object value;

    /**
     * Create instance
     *
     * @param operationType operation type
     * @param fieldNumber field number starting with 0
     * @param value operation value
     */
    public TupleUpdateOperation(TarantoolOperationType operationType, int fieldNumber, Object value) {
        if (fieldNumber < 0) {
            throw new IllegalArgumentException("Field number must be >= 0");
        }
        checkValue(operationType, value);
        this.operationType = operationType;
        this.fieldNumber = fieldNumber;
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
            throw new IllegalArgumentException("Filed name must be not empty");
        }
        checkValue(operationType, value);
        this.operationType = operationType;
        this.fieldName = fieldName;
        this.value = value;
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(
                Arrays.asList(getOperationType().toString(), getFieldNumber(), getValue()));
    }

    @Override
    public TarantoolOperationType getOperationType() {
        return operationType;
    }

    @Override
    public Integer getFieldNumber() {
        return fieldNumber;
    }

    @Override
    public void setFieldNumber(Integer fieldNumber) {
        this.fieldNumber = fieldNumber;
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
                    throw new IllegalArgumentException("The number of fields to remove must be >= 0");
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
