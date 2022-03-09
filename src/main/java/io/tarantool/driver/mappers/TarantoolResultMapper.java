package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.value.ArrayValue;

/**
 * Mapper from array of MessagePack tuples to {@link TarantoolResult}
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public class TarantoolResultMapper<T> extends AbstractResultMapper<TarantoolResult<T>> {
    /**
     * Basic constructor
     *
     * @param valueMapper value mapper to be used for result rows
     * @param tupleConverter MessagePack result array to tuple result converter
     * @param resultClass tuple result class
     */
    public TarantoolResultMapper(MessagePackValueMapper valueMapper,
                                 ValueConverter<ArrayValue, ? extends TarantoolResult<T>> tupleConverter,
                                 Class<? extends TarantoolResult<T>> resultClass) {
        super(valueMapper, tupleConverter, resultClass);
    }
}
