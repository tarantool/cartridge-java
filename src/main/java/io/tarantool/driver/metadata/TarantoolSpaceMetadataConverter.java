package io.tarantool.driver.metadata;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Iterator;

/**
 * Maps MessagePack {@link ArrayValue} into {@link TarantoolSpaceMetadata}
 *
 * @author Alexey Kuzin
 */
public class TarantoolSpaceMetadataConverter implements ValueConverter<ArrayValue, TarantoolSpaceMetadata> {

    private MessagePackValueMapper mapper;

    public TarantoolSpaceMetadataConverter(MessagePackValueMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolSpaceMetadata fromValue(ArrayValue value) {
        Iterator<Value> it = value.iterator();
        TarantoolSpaceMetadata metadata = new TarantoolSpaceMetadata();
        metadata.setSpaceId(mapper.fromValue(it.next().asIntegerValue()));
        metadata.setSpaceName(mapper.fromValue(it.next().asStringValue()));
        metadata.setOwnerId(mapper.fromValue(it.next().asIntegerValue()));
        return metadata;
    }
}
