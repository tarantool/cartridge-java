package io.tarantool.driver.metadata;

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
        ProxyTarantoolSpaceMetadataContainer proxyMetadata = new ProxyTarantoolSpaceMetadataContainer();

        Map<Value, Value> spacesMap = value.get(0).asMapValue().map();

        String spaceName = mapper.fromValue(spacesMap.get(SPACE_NAME_KEY).asStringValue());
        int spaceId = mapper.fromValue(spacesMap.get(SPACE_ID_KEY).asIntegerValue());

        TarantoolSpaceMetadata spaceMetadata = new TarantoolSpaceMetadata();
        spaceMetadata.setSpaceId(spaceId);
        spaceMetadata.setOwnerId(ID_UNKNOWN);
        spaceMetadata.setSpaceName(spaceName);

        List<Value> spaceFormat = spacesMap.get(SPACE_FORMAT_KEY).asArrayValue().list();
        spaceMetadata.setSpaceFormatMetadata(parseFormat(spaceFormat));

        Value indexesValue = spacesMap.get(SPACE_INDEX_KEY);
        if (indexesValue.isArrayValue() && indexesValue.asArrayValue().size() > 0) {
            List<Value> indexes = indexesValue.asArrayValue().list();
            proxyMetadata.addIndexes(spaceMetadata.getSpaceName(), parseIndexes(indexes));
        }

        proxyMetadata.addSpace(spaceMetadata);

        return proxyMetadata;
    }

    private LinkedHashMap<String, TarantoolFieldMetadata> parseFormat(List<Value> spaceFormat) {
        LinkedHashMap<String, TarantoolFieldMetadata> spaceFormatMetadata = new LinkedHashMap<>();

        int fieldPosition = 0;
        for (Value fieldValueMetadata : spaceFormat) {
            Map<Value, Value> fieldMap = fieldValueMetadata.asMapValue().map();
            spaceFormatMetadata.put(
                    mapper.fromValue(fieldMap.get(FORMAT_NAME_KEY).asStringValue()),
                    new TarantoolFieldMetadata(
                            mapper.fromValue(fieldMap.get(FORMAT_NAME_KEY).asStringValue()),
                            mapper.fromValue(fieldMap.get(FORMAT_TYPE_KEY).asStringValue()),
                            fieldPosition
                    )
            );
            fieldPosition++;
        }

        return spaceFormatMetadata;
    }

    private Map<String, TarantoolIndexMetadata> parseIndexes(List<Value> indexes) {
        Map<String, TarantoolIndexMetadata> indexMetadataMap = new HashMap<>();

        for (Value indexValueMetadata : indexes) {
            Map<Value, Value> indexMap = indexValueMetadata.asMapValue().map();

            int indexId = mapper.fromValue(indexMap.get(INDEX_ID_KEY).asIntegerValue());
            String indexName = mapper.fromValue(indexMap.get(INDEX_NAME_KEY).asStringValue());
            String indexType = mapper.fromValue(indexMap.get(INDEX_TYPE_KEY).asStringValue());
            boolean isUnique = mapper.fromValue(indexMap.get(INDEX_UNIQUE_KEY).asBooleanValue());

            TarantoolIndexOptions indexOptions = new TarantoolIndexOptions();
            indexOptions.setUnique(isUnique);

            TarantoolIndexMetadata indexMetadata = new TarantoolIndexMetadata();
            indexMetadata.setSpaceId(ID_UNKNOWN);
            indexMetadata.setIndexId(indexId);
            indexMetadata.setIndexType(TarantoolIndexType.fromString(indexType));
            indexMetadata.setIndexName(indexName);
            indexMetadata.setIndexOptions(indexOptions);

            List<Value> indexParts = indexMap.get(INDEX_PARTS_KEY).asArrayValue().list();
            List<TarantoolIndexPartMetadata> indexPartMetadata = indexParts.stream()
                    .map(parts -> {
                                int fieldNumber = mapper.fromValue(
                                        parts.asMapValue().map().get(INDEX_PARTS_FIELD_NO).asIntegerValue());
                                String fieldType = mapper.fromValue(
                                        parts.asMapValue().map().get(INDEX_PARTS_TYPE_KEY).asStringValue()
                                );
                                return new TarantoolIndexPartMetadata(fieldNumber - 1, fieldType);
                            }
                    ).collect(Collectors.toList());

            indexMetadata.setIndexParts(indexPartMetadata);

            indexMetadataMap.put(indexName, indexMetadata);
        }

        return indexMetadataMap;
    }
}
