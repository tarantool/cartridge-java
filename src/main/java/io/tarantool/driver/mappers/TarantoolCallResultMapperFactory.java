package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link TarantoolCallResultMapper} instances used for calling API functions on Tarantool instance
 *
 * @author Alexey Kuzin
 */
public class TarantoolCallResultMapperFactory extends AbstractTarantoolResultMapperFactory {

    /**
     * Basic constructor
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public TarantoolCallResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Basic constructor with empty mapper
     */
    public TarantoolCallResultMapperFactory() {
        super();
    }

    protected <T> TarantoolCallResultMapper<T> createMapper(ValueConverter<ArrayValue, T> valueConverter) {
        return new TarantoolCallResultMapper<>(new DefaultMessagePackMapper(), valueConverter);
    }

    @Override
    public TarantoolCallResultMapper<TarantoolTuple> withDefaultTupleValueConverter(
            TarantoolSpaceMetadata spaceMetadata) {
        return (TarantoolCallResultMapper<TarantoolTuple>) super.withDefaultTupleValueConverter(spaceMetadata);
    }

    @Override
    public <T> TarantoolCallResultMapper<T> withConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return (TarantoolCallResultMapper<T>) super.withConverter(valueConverter);
    }

    @Override
    public <T> TarantoolCallResultMapper<T> withConverter(Class<T> tupleClass,
                                                          ValueConverter<ArrayValue, T> valueConverter) {
        return (TarantoolCallResultMapper<T>) super.withConverter(tupleClass, valueConverter);
    }

    @Override
    public <T> TarantoolCallResultMapper<T> getByClass(Class<T> tupleClass) {
        return (TarantoolCallResultMapper<T>) super.getByClass(tupleClass);
    }
}
