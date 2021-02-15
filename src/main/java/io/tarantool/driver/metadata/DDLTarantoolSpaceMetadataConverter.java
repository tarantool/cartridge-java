package io.tarantool.driver.metadata;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Populates metadata from results of a call to proxy API function in Tarantool instance. The function result is
 * expected to have the format which is returned by DDL module.
 * See <a href="https://github.com/tarantool/ddl#input-data-format">
 * https://github.com/tarantool/ddl#input-data-format</a>
 *
 * @author Sergey Volgin
 * @author Artyom Dubinin
 */
public class DDLTarantoolSpaceMetadataConverter implements ValueConverter<Value, TarantoolMetadataContainer> {

    private static final int ID_UNKNOWN = -1;

    private static final StringValue SPACES_KEY = ValueFactory.newString("spaces");

    private static final StringValue SPACE_ID_KEY = ValueFactory.newString("id");
    private static final StringValue SPACE_FORMAT_KEY = ValueFactory.newString("format");
    private static final StringValue SPACE_INDEXES_KEY = ValueFactory.newString("indexes");

    private static final StringValue FORMAT_NAME_KEY = ValueFactory.newString("name");
    private static final StringValue FORMAT_TYPE_KEY = ValueFactory.newString("type");
    private static final StringValue FORMAT_IS_NULLABLE = ValueFactory.newString("is_nullable");

    private static final StringValue INDEX_NAME_KEY = ValueFactory.newString("name");
    private static final StringValue INDEX_UNIQUE_KEY = ValueFactory.newString("unique");
    private static final StringValue INDEX_TYPE_KEY = ValueFactory.newString("type");
    private static final StringValue INDEX_PARTS_KEY = ValueFactory.newString("parts");

    private static final StringValue INDEX_PARTS_TYPE_KEY = ValueFactory.newString("type");
    private static final StringValue INDEX_PARTS_PATH_KEY = ValueFactory.newString("path");

    public DDLTarantoolSpaceMetadataConverter() {
    }

    @Override
    public TarantoolMetadataContainer fromValue(Value value) {

        if (!value.isMapValue()) {
            throw new TarantoolClientException("Unsupported space metadata format: expected map");
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

            TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();
            spaceMetadata.setOwnerId(ID_UNKNOWN);
            spaceMetadata.setSpaceName(nameValue.asStringValue().asString());

            // FIXME Blocked by https://github.com/tarantool/ddl/issues/52
//            Value idValue = space.get(SPACE_ID_KEY);
//            if (idValue == null) {
//                throw new TarantoolClientException(
//                        "Unsupported space metadata format: key '" + SPACE_ID_KEY + "' not found");
//            }
//            spaceMetadata.setSpaceId(idValue.asIntegerValue().asInt());

            Value formatValue = space.get(SPACE_FORMAT_KEY);
            if (formatValue == null) {
                throw new TarantoolClientException(
                        "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' not found");
            }
            if (!formatValue.isArrayValue()) {
                throw new TarantoolClientException(
                        "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' value is not a list");
            }

            List<Value> spaceFormat = formatValue.asArrayValue().list();
            Map<String, TarantoolFieldMetadata> fields = parseFormat(spaceFormat);
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

    private Map<String, TarantoolFieldMetadata> parseFormat(List<Value> spaceFormat) {
        LinkedHashMap<String, TarantoolFieldMetadata> spaceFormatMetadata = new LinkedHashMap<>();

        int fieldPosition = 0;
        for (Value fieldValueMetadata : spaceFormat) {

            if (!fieldValueMetadata.isMapValue()) {
                throw new TarantoolClientException("Unsupported space metadata format: field metadata is not a map");
            }

            Map<Value, Value> fieldMap = fieldValueMetadata.asMapValue().map();
            Value fieldNameValue = fieldMap.get(FORMAT_NAME_KEY);
            if (fieldNameValue == null || !fieldNameValue.isStringValue()) {
                throw new TarantoolClientException(
                        "Unsupported space metadata format: key '" + FORMAT_NAME_KEY + "' must have string value");
            }
            String fieldName = fieldNameValue.asStringValue().asString();

            Value fieldTypeValue = fieldMap.get(FORMAT_TYPE_KEY);
            if (fieldTypeValue == null || !fieldTypeValue.isStringValue()) {
                throw new TarantoolClientException(
                        "Unsupported space metadata format: key '" + FORMAT_TYPE_KEY + "' must have string value");
            }
            String fieldType = fieldTypeValue.asStringValue().asString();

            Value fieldIsNullableValue = fieldMap.get(FORMAT_IS_NULLABLE);
            boolean fieldIsNullable = false;
            if (fieldIsNullableValue != null) {
                if (!fieldIsNullableValue.isBooleanValue()) {
                    throw new TarantoolClientException("Unsupported space metadata format: key '"
                            + FORMAT_IS_NULLABLE + "' must have boolean value");
                }
                fieldIsNullable = fieldIsNullableValue.asBooleanValue().getBoolean();
            }
            spaceFormatMetadata.put(fieldName,
                    new TarantoolFieldMetadata(fieldName, fieldType, fieldPosition++, fieldIsNullable));
        }

        return spaceFormatMetadata;
    }

    private Map<String, TarantoolIndexMetadata> parseIndexes(Map<String, TarantoolFieldMetadata> fields,
                                                             List<Value> indexes) {
        Map<String, TarantoolIndexMetadata> indexMetadataMap = new HashMap<>();

        int indexId = 0;
        for (Value indexValueMetadata : indexes) {

            if (!indexValueMetadata.isMapValue()) {
                throw new TarantoolClientException("Unsupported index metadata format: index metadata is not a map");
            }

            Map<Value, Value> indexMap = indexValueMetadata.asMapValue().map();

            Value indexNameValue = indexMap.get(INDEX_NAME_KEY);
            if (indexNameValue == null || !indexNameValue.isStringValue()) {
                throw new TarantoolClientException(
                        "Unsupported index metadata format: key '" + INDEX_NAME_KEY + "' must have string value");
            }
            String indexName = indexNameValue.asStringValue().asString();

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

            TarantoolIndexOptions indexOptions = new TarantoolIndexOptions();
            indexOptions.setUnique(isUnique);

            TarantoolIndexMetadata indexMetadata = new TarantoolIndexMetadata();
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

                        return new TarantoolIndexPartMetadata(fieldNumber - 1, fieldType, fieldPath);
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
}
