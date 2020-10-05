package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResultImpl;
import org.msgpack.value.ArrayValue;

/**
 * Mapper from array of MessagePack tuples to TarantoolResult
 *
 * @param <T> target tuple type
 * @author Alexey Kuzin
 */
public class TarantoolSimpleResultMapper<T> extends AbstractTarantoolResultMapper<T> {

    private MessagePackValueMapper valueMapper;

    /**
     * Basic constructor
     *
     * @param valueMapper value mapper to be used for tuple fields
     * @param tupleConverter MessagePack entity to tuple converter
     */
    public TarantoolSimpleResultMapper(MessagePackValueMapper valueMapper,
                                       ValueConverter<ArrayValue, T> tupleConverter) {
        super(valueMapper, v -> new TarantoolResultImpl<>(v, tupleConverter));
    }

}
