package io.tarantool.driver.core.metadata;

import io.tarantool.driver.api.metadata.TarantoolIndexType;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableStringValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps MessagePack {@link ArrayValue} into {@link TarantoolIndexMetadata}
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexMetadataConverter implements ValueConverter<ArrayValue, TarantoolIndexMetadata> {

    private static final long serialVersionUID = 20200708L;

    private static final ImmutableStringValue INDEX_FIELD_KEY = new ImmutableStringValueImpl("field");
    private static final ImmutableStringValue INDEX_TYPE_KEY = new ImmutableStringValueImpl("type");

    private final MessagePackValueMapper mapper;

    public TarantoolIndexMetadataConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolIndexMetadata fromValue(ArrayValue value) {
        Iterator<Value> it = value.iterator();
        TarantoolIndexMetadata metadata = new TarantoolIndexMetadata();
        metadata.setSpaceId(mapper.fromValue(it.next().asIntegerValue()));
        metadata.setIndexId(mapper.fromValue(it.next().asIntegerValue()));
        metadata.setIndexName(mapper.fromValue(it.next().asStringValue()));
        metadata.setIndexType(TarantoolIndexType.fromString(mapper.fromValue(it.next().asStringValue())));

        TarantoolIndexOptions indexOptions = new TarantoolIndexOptions();
        Map<String, Object> optionsMap = mapper.fromValue(it.next().asMapValue());
        indexOptions.setUnique((Boolean) optionsMap.get("unique"));

        metadata.setIndexOptions(indexOptions);

        ArrayValue indexPartsValue = it.next().asArrayValue();

        List<TarantoolIndexPartMetadata> indexParts = new ArrayList<>();

        //There are two index formats:
        // as array: [[0, 'unsigned'], [3, 'string'],...]
        // as map: [{'field' : 0, 'type' : 'unsigned'}, {'field' : 3, 'type' : 'string'}, ...]
        if (indexPartsValue.size() > 0) {
            if (indexPartsValue.get(0).isArrayValue()) {
                indexParts = indexPartsValue.list().stream()
                        .map(partValue -> new TarantoolIndexPartMetadata(
                                partValue.asArrayValue().get(0).asIntegerValue().asInt(),
                                partValue.asArrayValue().get(1).asStringValue().asString()
                        )).collect(Collectors.toList());
            } else {
                indexParts = indexPartsValue.list().stream()
                        .map(partValue -> new TarantoolIndexPartMetadata(
                                partValue.asMapValue().map().get(INDEX_FIELD_KEY).asIntegerValue().asInt(),
                                partValue.asMapValue().map().get(INDEX_TYPE_KEY).asStringValue().asString()
                        )).collect(Collectors.toList());
            }
        }

        metadata.setIndexParts(indexParts);

        return metadata;
    }
}
