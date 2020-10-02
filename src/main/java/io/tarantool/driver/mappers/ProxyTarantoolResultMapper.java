package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResultImpl;
import io.tarantool.driver.exceptions.TarantoolSpaceOperationException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.impl.ImmutableArrayValueImpl;

import java.util.Optional;

/**
 * Mapper from array of MessagePack tuples from proxy client to TarantoolResult
 *
 * @param <T> tuple target type
 * @author Sergey Volgin
 */
public class ProxyTarantoolResultMapper<T> implements MessagePackValueMapper {

    private final MessagePackValueMapper valueMapper;
    private final ValueConverter<ArrayValue, T> tupleConverter;

    /**
     * Basic constructor
     *
     * @param valueMapper    value mapper to be used for tuple fields
     * @param tupleConverter MessagePack entity to tuple converter
     */
    public ProxyTarantoolResultMapper(MessagePackValueMapper valueMapper,
                                      ValueConverter<ArrayValue, T> tupleConverter) {
        this.valueMapper = valueMapper;
        this.tupleConverter = tupleConverter;

        valueMapper.registerValueConverter(ArrayValue.class, TarantoolResultImpl.class,
                v -> {
                    ArrayValue tuplesValue = v;
                    // [nil, "Error msg..."]
                    if (v.size() == 2 && (v.get(0).isNilValue() && !v.get(1).isNilValue())) {
                        throw new TarantoolSpaceOperationException("Proxy operation error: %s", v.get(1));
                    }

                    // [[...], nil]
                    if (v.size() == 2 && (v.get(0).isArrayValue() && v.get(1).isNilValue())) {
                        tuplesValue = v.get(0).asArrayValue();
                    }

                    // [nil]
                    if (v.size() == 1 && v.get(0).isNilValue()) {
                        tuplesValue = ImmutableArrayValueImpl.empty();
                    }

                    // [[[],...]]
                    if (v.size() == 1 && v.get(0).isArrayValue()) {
                        boolean allElmIsArray = true;
                        for (Value elm : v.get(0).asArrayValue()) {
                            allElmIsArray = allElmIsArray && elm.isArrayValue();
                        }
                        if (allElmIsArray) {
                            tuplesValue = v.get(0).asArrayValue();
                        }
                    }

                    return new TarantoolResultImpl<>(tuplesValue, tupleConverter);
                });
    }

    public ValueConverter<ArrayValue, T> getTupleConverter() {
        return tupleConverter;
    }

    @Override
    public <V extends Value, O> O fromValue(V v) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v);
    }

    @Override
    public <V extends Value, O> O fromValue(V v, Class<O> targetClass) throws MessagePackValueMapperException {
        return valueMapper.fromValue(v, targetClass);
    }

    @Override
    public <V extends Value, O> void registerValueConverter(Class<V> valueClass, Class<O> objectClass,
                                                            ValueConverter<V, O> converter) {
        valueMapper.registerValueConverter(valueClass, objectClass, converter);
    }

    @Override
    public <V extends Value, O> Optional<ValueConverter<V, O>> getValueConverter(Class<V> entityClass,
                                                                                 Class<O> objectClass) {
        return valueMapper.getValueConverter(entityClass, objectClass);
    }
}
