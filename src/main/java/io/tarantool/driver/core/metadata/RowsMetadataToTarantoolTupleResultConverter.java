package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.core.TarantoolTupleResultImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.converters.ValueConverter;
import io.tarantool.driver.mappers.converters.value.ArrayValueToTarantoolTupleConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.MapValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Artyom Dubinin
 */
public class RowsMetadataToTarantoolTupleResultConverter
    implements ValueConverter<MapValue, TarantoolResult<TarantoolTuple>> {

    private static final long serialVersionUID = -5228606294087295535L;

    protected static final StringValue RESULT_META = ValueFactory.newString("metadata");
    protected static final StringValue RESULT_ROWS = ValueFactory.newString("rows");
    protected static final StringValue FORMAT_NAME_KEY = ValueFactory.newString("name");
    protected static final StringValue FORMAT_TYPE_KEY = ValueFactory.newString("type");

    private final ArrayValueToTarantoolTupleConverter tupleConverter;

    public RowsMetadataToTarantoolTupleResultConverter(ArrayValueToTarantoolTupleConverter tupleConverter) {
        super();
        this.tupleConverter = tupleConverter;
    }

    @Override
    public TarantoolResult<TarantoolTuple> fromValue(MapValue value) {
        Map<Value, Value> tupleMap = value.asMapValue().map();
        ArrayValue rawTuples = tupleMap.get(RESULT_ROWS).asArrayValue();
        ArrayValue rawMetadata = tupleMap.get(RESULT_META).asArrayValue();
        TarantoolSpaceMetadata parsedMetadata = parseCRUDMetadata(rawMetadata);

        return new TarantoolTupleResultImpl(rawTuples, parsedMetadata, tupleConverter);
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

    private TarantoolSpaceMetadata parseCRUDMetadata(Value metadata) {
        if (metadata == null || !metadata.isArrayValue()) {
            throw new TarantoolClientException("Unsupported crud metadata format: expected array of maps");
        }

        List<Value> responseFormat = metadata.asArrayValue().list();

        Map<String, TarantoolFieldMetadata> spaceFormatMetadata = new LinkedHashMap<>();
        int fieldPosition = 0;
        for (Value value : responseFormat) {
            if (!value.isMapValue()) {
                throw new TarantoolClientException("Unsupported crud metadata format: expected array of maps");
            }
            Map<Value, Value> fieldMap = value.asMapValue().map();

            Value fieldNameValue = fieldMap.get(FORMAT_NAME_KEY);
            if (fieldNameValue == null || !fieldNameValue.isStringValue()) {
                throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + FORMAT_NAME_KEY + "' must have string value," +
                        " get " + fieldNameValue);
            }
            String fieldName = fieldNameValue.asStringValue().asString();

            Value fieldTypeValue = fieldMap.get(FORMAT_TYPE_KEY);
            if (fieldTypeValue == null || !fieldTypeValue.isStringValue()) {
                throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + FORMAT_TYPE_KEY + "' must have string value," +
                        " get " + fieldNameValue);
            }
            String fieldType = fieldTypeValue.asStringValue().asString();

            spaceFormatMetadata.put(fieldName,
                new TarantoolFieldMetadataImpl(fieldName, fieldType, fieldPosition++));
        }

        TarantoolSpaceMetadataImpl spaceMetadata = new TarantoolSpaceMetadataImpl();
        spaceMetadata.setSpaceFormatMetadata(spaceFormatMetadata);

        return spaceMetadata;
    }
}
