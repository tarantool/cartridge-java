package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.api.metadata.TarantoolIndexType;
import io.tarantool.driver.api.metadata.TarantoolMetadataContainer;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolEmptyMetadataException;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.ValueType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Populates metadata from results of a call to proxy API function in Tarantool instance. The function result is
 * expected to have the format which is returned by DDL module.
 * See
 * <a href="https://github.com/tarantool/ddl#input-data-format">https://github.com/tarantool/ddl#input-data-format</a>
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public final class DDLTarantoolSpaceMetadataConverter implements ValueConverter<Value, TarantoolMetadataContainer> {

    private static final DDLTarantoolSpaceMetadataConverter instance =
        new DDLTarantoolSpaceMetadataConverter();

    private static final long serialVersionUID = -2100651651306707627L;

    private static final int ID_UNKNOWN = -1;

    private static final StringValue SPACES_KEY = ValueFactory.newString("spaces");

    private static final StringValue SPACE_ID_KEY = ValueFactory.newString("id");
    private static final StringValue SPACE_FORMAT_KEY = ValueFactory.newString("format");
    private static final StringValue SPACE_INDEXES_KEY = ValueFactory.newString("indexes");

    private static final StringValue INDEX_NAME_KEY = ValueFactory.newString("name");
    private static final StringValue INDEX_UNIQUE_KEY = ValueFactory.newString("unique");
    private static final StringValue INDEX_TYPE_KEY = ValueFactory.newString("type");
    private static final StringValue INDEX_PARTS_KEY = ValueFactory.newString("parts");

    private static final StringValue INDEX_PARTS_TYPE_KEY = ValueFactory.newString("type");
    private static final StringValue INDEX_PARTS_PATH_KEY = ValueFactory.newString("path");

    private static final ArrayValueToSpaceFormatConverter arrayValueToSpaceFormatConverter
        = ArrayValueToSpaceFormatConverter.getInstance();

    private DDLTarantoolSpaceMetadataConverter() {
    }

    @Override
    public TarantoolMetadataContainer fromValue(Value value) {

        if (!value.isMapValue()) {
            if (value.getValueType().equals(ValueType.ARRAY) && value.asArrayValue().size() == 0) {
                throw new TarantoolEmptyMetadataException();
            }
            throw new TarantoolClientException("Unsupported space metadata format: expected map, got %s",
                value.getValueType());
        }

        Map<Value, Value> spacesMap = value.asMapValue().map();

        if (!spacesMap.containsKey(SPACES_KEY) || !spacesMap.get(SPACES_KEY).isMapValue()) {
            throw new TarantoolClientException(
                "Unsupported metadata format: key '" + SPACES_KEY + "' must contain " +
                    "a map of spaces names to space metadata");
        }

        spacesMap = spacesMap.get(SPACES_KEY).asMapValue().map();

        ProxyTarantoolMetadataContainer proxyMetadata = new ProxyTarantoolMetadataContainer();
        for (Value nameValue : spacesMap.keySet()) {
            if (!nameValue.isStringValue()) {
                throw new TarantoolClientException(
                    "Unsupported metadata format: the spaces map keys must be of string type");
            }

            Value spaceValue = spacesMap.get(nameValue);
            if (!spaceValue.isMapValue()) {
                throw new TarantoolClientException(
                    "Unsupported metadata format: the spaces map values must be of map type");
            }
            Map<Value, Value> space = spaceValue.asMapValue().map();

            TarantoolSpaceMetadataImpl spaceMetadata = new TarantoolSpaceMetadataImpl();
            spaceMetadata.setOwnerId(ID_UNKNOWN);
            spaceMetadata.setSpaceName(nameValue.asStringValue().asString());

            Value formatValue = space.get(SPACE_FORMAT_KEY);
            if (formatValue == null) {
                throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' not found");
            }
            if (!formatValue.isArrayValue()) {
                throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' value is not a list");
            }

            ArrayValue spaceFormat = formatValue.asArrayValue();
            Map<String, TarantoolFieldMetadata> fields = arrayValueToSpaceFormatConverter.fromValue(spaceFormat);
            spaceMetadata.setSpaceFormatMetadata(fields);

            proxyMetadata.addSpace(spaceMetadata);

            Value indexesValue = space.get(SPACE_INDEXES_KEY);
            if (indexesValue != null && indexesValue.isArrayValue() && indexesValue.asArrayValue().size() > 0) {
                List<Value> indexes = indexesValue.asArrayValue().list();
                proxyMetadata.addIndexes(spaceMetadata.getSpaceName(), parseIndexes(fields, indexes));
            }
        }
        return proxyMetadata;
    }

    private Map<Object, TarantoolIndexMetadata> parseIndexes(
        Map<String, TarantoolFieldMetadata> fields,
        List<Value> indexes) {
        Map<Object, TarantoolIndexMetadata> indexMetadataMap = new HashMap<>();

        int indexId = 0;
        for (Value indexValueMetadata : indexes) {

            if (!indexValueMetadata.isMapValue()) {
                throw new TarantoolClientException("Unsupported index metadata format: index metadata is not a map");
            }

            Map<Value, Value> indexMap = indexValueMetadata.asMapValue().map();

            Value indexNameValue = indexMap.get(INDEX_NAME_KEY);

            Object indexName;
            if (indexNameValue != null && indexNameValue.isIntegerValue()) {
                indexName = indexNameValue.asIntegerValue().toInt();
            } else if (indexNameValue != null && indexNameValue.isStringValue()) {
                indexName = indexNameValue.asStringValue().asString();
            } else {
                throw new TarantoolClientException(
                    "Unsupported index metadata format: key '" + INDEX_NAME_KEY + "' must have string or int value");
            }

            Value indexTypeValue = indexMap.get(INDEX_TYPE_KEY);
            if (indexTypeValue == null || !indexTypeValue.isStringValue()) {
                throw new TarantoolClientException(
                    "Unsupported index metadata format: key '" + INDEX_TYPE_KEY + "' must have string value");
            }
            String indexType = indexTypeValue.asStringValue().asString();

            Value indexUniqueValue = indexMap.get(INDEX_UNIQUE_KEY);
            if (indexUniqueValue == null || !indexUniqueValue.isBooleanValue()) {
                throw new TarantoolClientException(
                    "Unsupported index metadata format: key '" + INDEX_UNIQUE_KEY + "' must have boolean value");
            }
            boolean isUnique = indexUniqueValue.asBooleanValue().getBoolean();

            TarantoolIndexOptionsImpl indexOptions = new TarantoolIndexOptionsImpl();
            indexOptions.setUnique(isUnique);

            TarantoolIndexMetadataImpl indexMetadata = new TarantoolIndexMetadataImpl();
            indexMetadata.setSpaceId(ID_UNKNOWN);
            indexMetadata.setIndexId(indexId++);
            indexMetadata.setIndexType(TarantoolIndexType.fromString(indexType));
            indexMetadata.setIndexName(indexName);
            indexMetadata.setIndexOptions(indexOptions);

            Value indexPartsValue = indexMap.get(INDEX_PARTS_KEY);
            if (indexPartsValue == null) {
                throw new TarantoolClientException(
                    "Unsupported index metadata format: key '" + INDEX_PARTS_KEY + "' not found");
            }
            if (!indexPartsValue.isArrayValue()) {
                throw new TarantoolClientException(
                    "Unsupported index metadata format: key '" + INDEX_PARTS_KEY + "' value is not a list");
            }

            List<Value> indexParts = indexPartsValue.asArrayValue().list();
            List<TarantoolIndexPartMetadata> indexPartMetadata = indexParts.stream()
                .map(parts -> {
                    if (!parts.isMapValue()) {
                        throw new TarantoolClientException(
                            "Unsupported index metadata format: index part metadata is not a map");
                    }

                    Map<Value, Value> partsMap = parts.asMapValue().map();

                    Value fieldPathValue = partsMap.get(INDEX_PARTS_PATH_KEY);
                    if (fieldPathValue == null || !fieldPathValue.isStringValue()) {
                        throw new TarantoolClientException("Unsupported index metadata format: key '" +
                            INDEX_PARTS_PATH_KEY + "' must have string value");
                    }
                    String fieldPath = fieldPathValue.asStringValue().asString();
                    int fieldNumber = getFieldNumberFromFieldPath(fields, fieldPath);

                    Value fieldTypeValue = partsMap.get(INDEX_PARTS_TYPE_KEY);
                    if (fieldTypeValue == null || !fieldTypeValue.isStringValue()) {
                        throw new TarantoolClientException("Unsupported index metadata format: key '" +
                            INDEX_PARTS_TYPE_KEY + "' must have string value");
                    }
                    String fieldType = fieldTypeValue.asStringValue().asString();

                    return new TarantoolIndexPartMetadataImpl(fieldNumber, fieldType, fieldPath);
                })
                .collect(Collectors.toList());

            indexMetadata.setIndexParts(indexPartMetadata);

            indexMetadataMap.put(indexName, indexMetadata);
        }

        return indexMetadataMap;
    }

    private int getFieldNumberFromFieldPath(Map<String, TarantoolFieldMetadata> fields, String fieldPath) {
        String fieldName = fieldPath;
        int dotPosition = fieldPath.indexOf('.');
        if (dotPosition >= 0) {
            fieldName = fieldPath.substring(0, dotPosition);
        }
        TarantoolFieldMetadata fieldMeta = fields.get(fieldName);
        if (fieldMeta != null) {
            return fieldMeta.getFieldPosition();
        } else {
            try {
                return Integer.parseInt(fieldName);
            } catch (NumberFormatException e) {
                throw new TarantoolClientException("Unsupported index metadata format: " +
                    "unable to determine the field number or name from path %s", fieldPath);
            }
        }
    }

    public static DDLTarantoolSpaceMetadataConverter getInstance() {
        return instance;
    }
}
