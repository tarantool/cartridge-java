package io.tarantool.driver.mappers;

import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link TarantoolSimpleResultMapper} instances used for handling box protocol operation results
 *
 * @author Alexey Kuzin
 */
public class TarantoolSimpleResultMapperFactory extends AbstractTarantoolResultMapperFactory {

    /**
     * Basic constructor
     *
     * @param messagePackMapper mapper for MessagePack entities in tuple fields to Java objects
     */
    public TarantoolSimpleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Basic constructor with empty mapper
     */
    public TarantoolSimpleResultMapperFactory() {
        super();
    }

    protected <T> TarantoolSimpleResultMapper<T> createMapper(ValueConverter<ArrayValue, T> valueConverter) {
        return new TarantoolSimpleResultMapper<>(new DefaultMessagePackMapper(), valueConverter);
    }

    @Override
    public TarantoolSimpleResultMapper<TarantoolTuple> withDefaultTupleValueConverter(
            TarantoolSpaceMetadata spaceMetadata) {
        return (TarantoolSimpleResultMapper<TarantoolTuple>) super.withDefaultTupleValueConverter(spaceMetadata);
    }

    @Override
    public <T> TarantoolSimpleResultMapper<T> withConverter(ValueConverter<ArrayValue, T> valueConverter) {
        return (TarantoolSimpleResultMapper<T>) super.withConverter(valueConverter);
    }

    @Override
    public <T> TarantoolSimpleResultMapper<T> withConverter(Class<T> tupleClass,
                                                            ValueConverter<ArrayValue, T> valueConverter) {
        return (TarantoolSimpleResultMapper<T>) super.withConverter(tupleClass, valueConverter);
    }
}
