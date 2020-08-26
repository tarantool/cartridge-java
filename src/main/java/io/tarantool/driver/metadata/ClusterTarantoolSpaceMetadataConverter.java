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
 * Maps MessagePack {@link ArrayValue} form router proxy into {@link ClusterTarantoolSpaceMetadataContainer}
 * See <a href="https://github.com/tarantool/ddl">https://github.com/tarantool/ddl</a>
 *
 * @author Sergey Volgin
 */
public class ClusterTarantoolSpaceMetadataConverter
        implements ValueConverter<ArrayValue, ClusterTarantoolSpaceMetadataContainer> {

    private static final int ID_UNKNOWN = -1;

    private static final ImmutableStringValue SPACES_KEY = new ImmutableStringValueImpl("spaces");
    private static final ImmutableStringValue FORMAT_FIELD_KEY = new ImmutableStringValueImpl("format");
    private static final ImmutableStringValue FORMAT_NAME_KEY = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue FORMAT_TYPE_KEY = new ImmutableStringValueImpl("type");
    private static final ImmutableStringValue INDEXES_FIELD_KEY = new ImmutableStringValueImpl("indexes");
    private static final ImmutableStringValue INDEX_NAME_KEY = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue INDEX_UNIQUE_KEY = new ImmutableStringValueImpl("unique");
    private static final ImmutableStringValue INDEX_TYPE_KEY = new ImmutableStringValueImpl("type");
    private static final ImmutableStringValue INDEX_PARTS_KEY = new ImmutableStringValueImpl("parts");
    private static final ImmutableStringValue INDEX_PARTS_PATH_KEY = new ImmutableStringValueImpl("path");
    private static final ImmutableStringValue INDEX_PARTS_TYPE_KEY = new ImmutableStringValueImpl("type");

    private MessagePackValueMapper mapper;

    public ClusterTarantoolSpaceMetadataConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ClusterTarantoolSpaceMetadataContainer fromValue(ArrayValue value) {

        ClusterTarantoolSpaceMetadataContainer proxyMetadata = new ClusterTarantoolSpaceMetadataContainer();

        Map<Value, Value> spacesMap = value.get(0).asMapValue().map().get(SPACES_KEY).asMapValue().map();

        for (Map.Entry<Value, Value> spaceValue : spacesMap.entrySet()) {
            String spaceName = mapper.fromValue(spaceValue.getKey().asStringValue());

            TarantoolSpaceMetadata metadata = new TarantoolSpaceMetadata();
            metadata.setSpaceId(ID_UNKNOWN);
            metadata.setOwnerId(ID_UNKNOWN);
            metadata.setSpaceName(spaceName);

            Map<Value, Value> spaceAttr = spaceValue.getValue().asMapValue().map();

            List<Value> spaceFormat = spaceAttr.get(FORMAT_FIELD_KEY).asArrayValue().list();

            metadata.setSpaceFormatMetadata(parseFormat(spaceFormat));

            List<Value> indexes = spaceAttr.get(INDEXES_FIELD_KEY).asArrayValue().list();

            proxyMetadata.addSpace(metadata);
            proxyMetadata.addIndexes(metadata.getSpaceName(), parseIndexes(indexes, metadata));
        }

        return proxyMetadata;
    }

    private LinkedHashMap<String, TarantoolFieldFormatMetadata> parseFormat(List<Value> spaceFormat) {
        LinkedHashMap<String, TarantoolFieldFormatMetadata> spaceFormatMetadata = new LinkedHashMap<>();

        int fieldPosition = 0;
        for (Value fieldValueMetadata : spaceFormat) {
            Map<Value, Value> fieldMap = fieldValueMetadata.asMapValue().map();
            spaceFormatMetadata.put(
                    mapper.fromValue(fieldMap.get(FORMAT_NAME_KEY).asStringValue()),
                    new TarantoolFieldFormatMetadata(
                            mapper.fromValue(fieldMap.get(FORMAT_NAME_KEY).asStringValue()),
                            mapper.fromValue(fieldMap.get(FORMAT_TYPE_KEY).asStringValue()),
                            fieldPosition
                    )
            );
            fieldPosition++;
        }

        return spaceFormatMetadata;
    }

    private Map<String, TarantoolIndexMetadata> parseIndexes(List<Value> indexes,
                                                             TarantoolSpaceMetadata spaceMetadata) {

        Map<String, TarantoolIndexMetadata> indexMetadataMap = new HashMap<>();

        int indexId = 0;
        for (Value indexValueMetadata : indexes) {
            Map<Value, Value> indexMap = indexValueMetadata.asMapValue().map();

            String indexName = mapper.fromValue(indexMap.get(INDEX_NAME_KEY).asStringValue());

            //TODO index parts
            TarantoolIndexMetadata indexMetadata = new TarantoolIndexMetadata();
            indexMetadata.setSpaceId(ID_UNKNOWN);
            indexMetadata.setIndexId(indexId);
            indexMetadata.setIndexType(TarantoolIndexType.fromString(
                    mapper.fromValue(indexMap.get(INDEX_TYPE_KEY).asStringValue())));
            indexMetadata.setIndexName(mapper.fromValue(indexMap.get(INDEX_NAME_KEY).asStringValue()));

            TarantoolIndexOptions indexOptions = new TarantoolIndexOptions();
            indexOptions.setUnique(mapper.fromValue(indexMap.get(INDEX_UNIQUE_KEY).asBooleanValue()));

            indexMetadata.setIndexOptions(indexOptions);

            List<Value> indexParts = indexMap.get(INDEX_PARTS_KEY).asArrayValue().list();
            List<TarantoolIndexPartMetadata> indexPartMetadata = indexParts.stream()
                    .map(parts -> {
                        String fieldName =
                                parts.asMapValue().map().get(INDEX_PARTS_PATH_KEY).asStringValue().asString();
                        return new TarantoolIndexPartMetadata(
                                spaceMetadata.getFieldPositionByName(fieldName),
                                parts.asMapValue().map().get(INDEX_PARTS_TYPE_KEY).asStringValue().asString()
                        );
                    }
            ).collect(Collectors.toList());

            indexMetadata.setIndexParts(indexPartMetadata);

            indexMetadataMap.put(indexName, indexMetadata);

            indexId++;
        }

        return indexMetadataMap;
    }
}
