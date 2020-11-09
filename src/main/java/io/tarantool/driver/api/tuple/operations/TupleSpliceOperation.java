package io.tarantool.driver.api.tuple.operations;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;

import java.util.Arrays;

/**
 * Represent splice operation on tuple field
 *
 * @author Sergey Volgin
 */
public class TupleSpliceOperation extends TupleUpdateOperation {

    private final int position;
    private final int offset;

    public TupleSpliceOperation(int fieldIndex, int position, int offset, String value) {
        super(TarantoolUpdateOperationType.SPLICE, fieldIndex, value);
        this.position = position;
        this.offset = offset;
    }

    public TupleSpliceOperation(String fieldName, int position, int offset, String value) {
        super(TarantoolUpdateOperationType.SPLICE, fieldName, value);
        this.position = position;
        this.offset = offset;
    }

    private TupleSpliceOperation(TarantoolUpdateOperationType operationType, Integer fieldIndex,
                                 String fieldName, Object value, int position, int offset,
                                 boolean isProxyOperation) {
        super(operationType, fieldIndex, fieldName, value, isProxyOperation);
        this.position = position;
        this.offset = offset;
    }

    @Override
    public TupleOperation toProxyTupleOperation() {
        return new TupleSpliceOperation(
                this.getOperationType(),
                this.getFieldNumber(),
                this.getFieldName(),
                this.getValue(),
                this.getPosition(),
                this.getOffset(),
                true
        );
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(Arrays.asList(
                getOperationType().toString(), getFieldIndex(), getPosition(), getOffset(), getValue()));
    }

    public int getPosition() {
        return position;
    }

    public int getOffset() {
        return offset;
    }
}
