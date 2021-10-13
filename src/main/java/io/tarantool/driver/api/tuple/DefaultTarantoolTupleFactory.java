package io.tarantool.driver.api.tuple;

import io.tarantool.driver.core.tuple.TarantoolTupleImpl;
import io.tarantool.driver.mappers.MessagePackMapper;

import java.util.Arrays;
import java.util.Collection;

/**
 * Default implementation for {@link TarantoolTupleFactory}
 *
 * @author Alexey Kuzin
 */
public class DefaultTarantoolTupleFactory implements TarantoolTupleFactory {

    private final MessagePackMapper mapper;

    public DefaultTarantoolTupleFactory(MessagePackMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public TarantoolTuple create() {
        return new TarantoolTupleImpl(mapper);
    }

    @Override
    public TarantoolTuple create(Object... fields) {
        return new TarantoolTupleImpl(Arrays.asList(fields), mapper);
    }

    @Override
    public TarantoolTuple create(Collection<?> fields) {
        return new TarantoolTupleImpl(fields, mapper);
    }
}
