package io.tarantool.driver.protocol;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.Value;

/**
 * Classes implementing this interface can be converted into MessagePack representation
 *
 * @author Alexey Kuzin
 */
public interface Packable {
    /**
     * Convert this instance into a corresponding MessagePack {@link Value}
     * @param mapper configured Java objects to entities mapper
     * @return MessagePack entity
     */
    Value toMessagePackValue(MessagePackObjectMapper mapper);
}
