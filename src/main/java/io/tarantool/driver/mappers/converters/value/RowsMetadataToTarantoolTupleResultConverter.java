package io.tarantool.driver.mappers.converters.value;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolResultImpl;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Map;

/**
 * @author Artyom Dubinin
 */
public class RowsMetadataToTarantoolTupleResultConverter
    implements ValueConverter<MapValue, TarantoolResult<TarantoolTuple>> {

    private static final long serialVersionUID = -5228606294087295535L;

    protected static final StringValue RESULT_META = ValueFactory.newString("metadata");
    protected static final StringValue RESULT_ROWS = ValueFactory.newString("rows");

    private final ArrayValueToTarantoolTupleConverter tupleConverter;

    public RowsMetadataToTarantoolTupleResultConverter(ArrayValueToTarantoolTupleConverter tupleConverter) {
        super();
        this.tupleConverter = tupleConverter;
    }

    @Override
    public TarantoolResult<TarantoolTuple> fromValue(MapValue value) {
        Map<Value, Value> tupleMap = value.asMapValue().map();
        ArrayValue rawTuples = tupleMap.get(RESULT_ROWS).asArrayValue();

        return new TarantoolResultImpl<>(rawTuples, tupleConverter);
    }

    @Override
    public boolean canConvertValue(MapValue value) {
        // [{"metadata" : [...], "rows": [...]}]
        Map<Value, Value> tupleMap = value.asMapValue().map();
        if (!hasRowsAndMetadata(tupleMap)) {
            return false;
        }
        return tupleMap.get(RESULT_ROWS).isArrayValue() && tupleMap.get(RESULT_META).isArrayValue();
    }

    static boolean hasRowsAndMetadata(Map<Value, Value> valueMap) {
        return valueMap.containsKey(RESULT_META) && valueMap.containsKey(RESULT_ROWS);
    }
}
