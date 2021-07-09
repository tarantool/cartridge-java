package io.tarantool.driver.mappers;

import io.tarantool.driver.protocol.Packable;
import org.msgpack.value.Value;

/**
 * Default converter for internal classes aware of MessagePack serialization
 *
 * @author Alexey Kuzin
 */
public class DefaultPackableObjectConverter implements ObjectConverter<Packable, Value> {

    private static final long serialVersionUID = 20200708L;

    private final MessagePackObjectMapper mapper;

    public DefaultPackableObjectConverter(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Value toValue(Packable object) {
        return object.toMessagePackValue(mapper);
    }
}
