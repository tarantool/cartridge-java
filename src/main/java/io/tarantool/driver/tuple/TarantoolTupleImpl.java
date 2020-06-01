package io.tarantool.driver.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

/**
 * Basic Tarantool tuple implementation
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleImpl implements TarantoolTuple {

    private List<TarantoolField<?, ? extends Value>> fields;

    public TarantoolTupleImpl(ArrayValue value, MessagePackObjectMapper mapper) {
        this.fields = new ArrayList<>(value.size());
        for (Value fieldValue: value) {
            if (fieldValue.isNilValue()) {
                fields.add(new TarantoolNullField<>());
            } else {
                fields.add(new TarantoolFieldImpl<>(value, mapper));
            }
        }
    }

    @Override
    public Optional<TarantoolField<?, ? extends Value>> get(int fieldPosition) {
        Assert.state(fieldPosition >= 0, "Field position starts with 0");

        if (fieldPosition < fields.size()) {
            return Optional.ofNullable(fields.get(fieldPosition));
        }
        return Optional.empty();
    }

    @Override
    public Iterator<TarantoolField<?, ? extends Value>> iterator() {
        return fields.iterator();
    }

    @Override
    public void forEach(Consumer<? super TarantoolField<?, ? extends Value>> action) {
        fields.forEach(action);
    }

    @Override
    public Spliterator<TarantoolField<?, ? extends Value>> spliterator() {
        return fields.spliterator();
    }

    @Override
    public ArrayValue toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(fields);
    }
}
