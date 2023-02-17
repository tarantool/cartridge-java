package io.tarantool.driver.core.metadata;

import java.util.Map;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolResultFactory;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import static io.tarantool.driver.core.TarantoolResultFactory.getInstance;

/**
 * @author Artyom Dubinin
 */
public class RowsMetadataToTarantoolTupleResultConverter
    implements ValueConverter<MapValue, TarantoolResult<TarantoolTuple>> {

    private static final long serialVersionUID = -5228606294087295535L;

    protected static final StringValue RESULT_META = ValueFactory.newString("metadata");
    protected static final StringValue RESULT_ROWS = ValueFactory.newString("rows");

    protected static final CRUDResponseToTarantoolSpaceMetadataConverter spaceMetadataConverter =
        CRUDResponseToTarantoolSpaceMetadataConverter.getInstance();

    private final ArrayValueToTarantoolTupleConverter tupleConverter;
    private final TarantoolResultFactory tarantoolResultFactory;

    public RowsMetadataToTarantoolTupleResultConverter(ArrayValueToTarantoolTupleConverter tupleConverter) {
        super();
        this.tupleConverter = tupleConverter;
        this.tarantoolResultFactory = getInstance();
    }

    @Override
    public TarantoolResult<TarantoolTuple> fromValue(MapValue value) {
        Map<Value, Value> tupleMap = value.asMapValue().map();
        ArrayValue rawTuples = tupleMap.get(RESULT_ROWS).asArrayValue();
        ArrayValue rawMetadata = tupleMap.get(RESULT_META).asArrayValue();
        TarantoolSpaceMetadata parsedMetadata = spaceMetadataConverter.fromValue(rawMetadata);

        return tarantoolResultFactory.createTarantoolTupleResultImpl(rawTuples, parsedMetadata, tupleConverter);
    }

    @Override
    public boolean canConvertValue(MapValue value) {
        // {"metadata" : [...], "rows": [...]}
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
