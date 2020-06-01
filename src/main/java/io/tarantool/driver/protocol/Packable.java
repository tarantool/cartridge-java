package io.tarantool.driver.protocol;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;

/**
 * Classes implementing this interface can be converted into MessagePack representation
 */
public interface Packable<T extends Value> {
    /**
     * Convert this instance into a corresponding MessagePack {@link Value}
     * @param mapper configured Java objects to entities mapper
     * @return MessagePack entity
     */
    T toMessagePackValue(MessagePackObjectMapper mapper);
}
