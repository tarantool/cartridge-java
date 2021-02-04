package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import org.msgpack.value.ArrayValue;

/**
 * Factory for {@link TarantoolResultMapper} instances used for handling results with tuples of any type
 *
 * @author Alexey Kuzin
 */
public class TupleResultMapperFactory<T> extends TarantoolResultMapperFactory<T> {

    public TupleResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(ValueConverter<ArrayValue, T> tupleConverter) {
        return withConverter(new TarantoolResultConverter<>(tupleConverter));
    }

    /**
     * Get converter for tuples in {@link TarantoolResult}
     *
     * @param resultClass allows to specify the result type in case if it is impossible to get it via reflection
     *                    (e.g. lambda)
     * @param tupleConverter MessagePack-to-entity converter for tuples
     * @return mapper instance
     */
    public TarantoolResultMapper<T> withTupleValueConverter(ValueConverter<ArrayValue, T> tupleConverter,
                                                            Class<? extends TarantoolResult<T>> resultClass) {
        return withConverter(resultClass, new TarantoolResultConverter<>(tupleConverter));
    }
}
