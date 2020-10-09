package io.tarantool.driver.metadata;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableStringValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps MessagePack {@link ArrayValue} from proxy schema API function call result into
 * {@link ProxyTarantoolSpaceMetadataContainer}.
 * See <a href="https://github.com/tarantool/ddl">https://github.com/tarantool/ddl</a>
 *
 * @author Sergey Volgin
 */
public class ProxyTarantoolSpaceMetadataConverter
        implements ValueConverter<ArrayValue, ProxyTarantoolSpaceMetadataContainer> {

    private static final int ID_UNKNOWN = -1;

    private static final ImmutableStringValue SPACE_ID_KEY = new ImmutableStringValueImpl("id");
    private static final ImmutableStringValue SPACE_NAME_KEY = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue SPACE_FORMAT_KEY = new ImmutableStringValueImpl("_format");
    private static final ImmutableStringValue SPACE_INDEX_KEY = new ImmutableStringValueImpl("index");

    private static final ImmutableStringValue FORMAT_NAME_KEY = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue FORMAT_TYPE_KEY = new ImmutableStringValueImpl("type");

    private static final ImmutableStringValue INDEX_ID_KEY = new ImmutableStringValueImpl("id");
    private static final ImmutableStringValue INDEX_NAME_KEY = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue INDEX_UNIQUE_KEY = new ImmutableStringValueImpl("unique");
    private static final ImmutableStringValue INDEX_TYPE_KEY = new ImmutableStringValueImpl("type");
    private static final ImmutableStringValue INDEX_PARTS_KEY = new ImmutableStringValueImpl("parts");

    private static final ImmutableStringValue INDEX_PARTS_FIELD_NO = new ImmutableStringValueImpl("fieldno");
    private static final ImmutableStringValue INDEX_PARTS_TYPE_KEY = new ImmutableStringValueImpl("type");

    private final MessagePackValueMapper mapper;

    public ProxyTarantoolSpaceMetadataConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ProxyTarantoolSpaceMetadataContainer fromValue(ArrayValue value) {

        if (value.size() == 0) {
            throw new TarantoolClientException("Empty tuple returned for space metadata");
        }

        if (!value.get(0).isMapValue()) {
            throw new TarantoolClientException("Unsupported space metadata format: expected map");
        }

        Map<Value, Value> spacesMap = value.get(0).asMapValue().map();

        Value nameValue = spacesMap.get(SPACE_NAME_KEY);
        if (nameValue == null) {
            throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_NAME_KEY + "' not found");
        }

        Value idValue = spacesMap.get(SPACE_ID_KEY);
        if (idValue == null) {
            throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_ID_KEY + "' not found");
        }

        ProxyTarantoolSpaceMetadataContainer proxyMetadata = new ProxyTarantoolSpaceMetadataContainer();

        TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();
        spaceMetadata.setSpaceId(idValue.asIntegerValue().asInt());
        spaceMetadata.setOwnerId(ID_UNKNOWN);
        spaceMetadata.setSpaceName(nameValue.asStringValue().asString());

        Value formatValue = spacesMap.get(SPACE_FORMAT_KEY);
        if (formatValue == null) {
            throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' not found");
        }
        if (!formatValue.isArrayValue()) {
            throw new TarantoolClientException(
                    "Unsupported space metadata format: key '" + SPACE_FORMAT_KEY + "' value is not a list");
        }

        List<Value> spaceFormat = formatValue.asArrayValue().list();
        spaceMetadata.setSpaceFormatMetadata(parseFormat(spaceFormat));

        proxyMetadata.addSpace(spaceMetadata);

        Value indexesValue = spacesMap.get(SPACE_INDEX_KEY);
        if (indexesValue != null && indexesValue.isArrayValue() && indexesValue.asArrayValue().size() > 0) {
            List<Value> indexes = indexesValue.asArrayValue().list();
            proxyMetadata.addIndexes(spaceMetadata.getSpaceName(), parseIndexes(indexes));
        }

        return proxyMetadata;
    }

    private LinkedHashMap<String, TarantoolFieldMetadata> parseFormat(List<Value> spaceFormat) {
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

            Value fieldTypeValue = fieldMap.get(FORMAT_NAME_KEY);
            if (fieldTypeValue == null || !fieldTypeValue.isStringValue()) {
                throw new TarantoolClientException(
                        "Unsupported space metadata format: key '" + FORMAT_TYPE_KEY + "' must have string value");
            }
            String fieldType = fieldTypeValue.asStringValue().asString();

            spaceFormatMetadata.put(fieldName, new TarantoolFieldMetadata(fieldName, fieldType, fieldPosition++));
        }

        return spaceFormatMetadata;
    }

    private Map<String, TarantoolIndexMetadata> parseIndexes(List<Value> indexes) {
        Map<String, TarantoolIndexMetadata> indexMetadataMap = new HashMap<>();

        for (Value indexValueMetadata : indexes) {

            if (!indexValueMetadata.isMapValue()) {
                throw new TarantoolClientException("Unsupported index metadata format: index metadata is not a map");
            }

            Map<Value, Value> indexMap = indexValueMetadata.asMapValue().map();
            Value indexIdValue = indexMap.get(INDEX_ID_KEY);
            if (indexIdValue == null || !indexIdValue.isIntegerValue()) {
                throw new TarantoolClientException(
                        "Unsupported index metadata format: key '" + INDEX_ID_KEY + "' must have int value");
            }
            int indexId = indexIdValue.asIntegerValue().asInt();

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
            indexMetadata.setIndexId(indexId);
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
                        Value fieldPositionValue = partsMap.get(INDEX_PARTS_FIELD_NO);
                        if (fieldPositionValue == null || !fieldPositionValue.isIntegerValue()) {
                            throw new TarantoolClientException("Unsupported index metadata format: key '" +
                                    INDEX_PARTS_FIELD_NO + "' must have int value");
                        }
                        int fieldNumber = fieldPositionValue.asIntegerValue().asInt();

                        Value fieldTypeValue = partsMap.get(INDEX_PARTS_TYPE_KEY);
                        if (fieldTypeValue == null || !fieldTypeValue.isStringValue()) {
                            throw new TarantoolClientException("Unsupported index metadata format: key '" +
                                    INDEX_PARTS_TYPE_KEY + "' must have string value");
                        }
                        String fieldType = fieldTypeValue.asStringValue().asString();

                        return new TarantoolIndexPartMetadata(fieldNumber - 1, fieldType);
                    })
                    .collect(Collectors.toList());

            indexMetadata.setIndexParts(indexPartMetadata);

            indexMetadataMap.put(indexName, indexMetadata);
        }

        return indexMetadataMap;
    }
}
