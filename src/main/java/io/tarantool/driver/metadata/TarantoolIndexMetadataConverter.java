package io.tarantool.driver.metadata;

import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.mappers.ValueConverter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.Iterator;

/**
 * Maps MessagePack {@link ArrayValue} into {@link TarantoolIndexMetadata}
 *
 * @author Alexey Kuzin
 */
public class TarantoolIndexMetadataConverter implements ValueConverter<ArrayValue, TarantoolIndexMetadata> {

    private MessagePackValueMapper mapper;

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
        return metadata;
    }
}
