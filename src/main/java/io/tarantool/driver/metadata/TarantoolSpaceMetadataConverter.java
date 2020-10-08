package io.tarantool.driver.metadata;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.ImmutableStringValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps MessagePack {@link ArrayValue} into {@link TarantoolSpaceMetadata}
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceMetadataConverter implements ValueConverter<ArrayValue, TarantoolSpaceMetadata> {

    private static final ImmutableStringValue FORMAT_FIELD_NAME = new ImmutableStringValueImpl("name");
    private static final ImmutableStringValue FORMAT_FIELD_TYPE = new ImmutableStringValueImpl("type");

    private MessagePackValueMapper mapper;

    public TarantoolSpaceMetadataConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolSpaceMetadata fromValue(ArrayValue value) {
        Iterator<Value> it = value.iterator();
        TarantoolSpaceMetadata metadata = new TarantoolSpaceMetadata();
        metadata.setSpaceId(mapper.fromValue(it.next().asIntegerValue()));
        metadata.setOwnerId(mapper.fromValue(it.next().asIntegerValue()));
        metadata.setSpaceName(mapper.fromValue(it.next().asStringValue()));

        Value spaceMetadataValue =  it.next();
        while (!spaceMetadataValue.isArrayValue()) {
            spaceMetadataValue =  it.next();
        }

        LinkedHashMap<String, TarantoolFieldMetadata> spaceFormatMetadata = new LinkedHashMap<>();

        int fieldPosition = 0;
        for (Value fieldValueMetadata : spaceMetadataValue.asArrayValue()) {
            Map<Value, Value> fieldMap = fieldValueMetadata.asMapValue().map();
            spaceFormatMetadata.put(
                    fieldMap.get(FORMAT_FIELD_NAME).toString(),
                    new TarantoolFieldMetadata(
                            fieldMap.get(FORMAT_FIELD_NAME).toString(),
                            fieldMap.get(FORMAT_FIELD_TYPE).toString(),
                            fieldPosition
                    )
            );
            fieldPosition++;
        }

        metadata.setSpaceFormatMetadata(spaceFormatMetadata);

        return metadata;
    }
}
