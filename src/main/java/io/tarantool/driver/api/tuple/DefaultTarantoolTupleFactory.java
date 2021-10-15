package io.tarantool.driver.api.tuple;

import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;

import java.util.Arrays;
import java.util.Collection;

/**
 * Default implementation for {@link TarantoolTupleFactory}
 *
 * @author Alexey Kuzin
 */
public class DefaultTarantoolTupleFactory implements TarantoolTupleFactory {

    private final MessagePackMapper mapper;
    private TarantoolSpaceMetadata metadata;

    /**
     * Constructor. Allows creating tuples without a space metadata. Created tuples will use the passed mapper
     * for serialization into MessagePack arrays.
     *
     * @param mapper mapper for conversion between Java objects and MessagePack entities
     */
    public DefaultTarantoolTupleFactory(MessagePackMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Constructor with a space metadata. Allows accessing the tuple fields by space fields' names. Created tuples will
     * use the passed mapper for serialization into MessagePack arrays.
     *
     * @param mapper   mapper for conversion between Java objects and MessagePack entities
     * @param metadata space metadata for mapping the tuple fields to space fields
     */
    public DefaultTarantoolTupleFactory(MessagePackMapper mapper, TarantoolSpaceMetadata metadata) {
        this.mapper = mapper;
        this.metadata = metadata;
    }

    @Override
    public TarantoolTuple create() {
        return new TarantoolTupleImpl(mapper, metadata);
    }

    @Override
    public TarantoolTuple create(Object... fields) {
        return new TarantoolTupleImpl(Arrays.asList(fields), mapper, metadata);
    }

    @Override
    public TarantoolTuple create(Collection<?> fields) {
        return new TarantoolTupleImpl(fields, mapper, metadata);
    }
}
