package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.exceptions.TarantoolValueConverterNotFoundException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
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
 * @author Sergey Volgin
 */
public class TarantoolTupleImpl implements TarantoolTuple {

    private TarantoolSpaceMetadata spaceMetadata;

    private List<TarantoolField> fields;

    private final MessagePackValueMapper valueMapper;

    private final MessagePackObjectMapper objectMapper;

    /**
     * @param value messagePack entity
     * @param mapperFactory for getting convectors between entity and Java objects
     */
    public TarantoolTupleImpl(ArrayValue value, DefaultMessagePackMapperFactory mapperFactory) {
        this(value, mapperFactory, null);
    }

    /**
     * Basic constructor.
     *
     * @param value messagePack entity
     * @param mapperFactory for getting convectors between entity and Java objects
     * @param spaceMetadata tarantool space metadata {@link TarantoolSpaceMetadata}
     */
    public TarantoolTupleImpl(ArrayValue value, DefaultMessagePackMapperFactory mapperFactory, TarantoolSpaceMetadata spaceMetadata) {
        this.spaceMetadata = spaceMetadata;
        this.valueMapper = mapperFactory.defaultComplexTypesMapper();
        this.objectMapper = mapperFactory.defaultComplexTypesMapper();

        this.fields = new ArrayList<>(value.size());
        for (Value fieldValue : value) {
            if (fieldValue.isNilValue()) {
                fields.add(new TarantoolNullField());
            } else {
                fields.add(new TarantoolFieldImpl<>(fieldValue, valueMapper));
            }
        }
    }

    @Override
    public Optional<TarantoolField> getField(int fieldPosition) {
        Assert.state(fieldPosition >= 0, "Field position starts with 0");

        if (fieldPosition < fields.size()) {
            return Optional.ofNullable(fields.get(fieldPosition));
        }
        return Optional.empty();
    }

    @Override
    public Optional<TarantoolField> getField(String fieldName) {
        int fieldPosition = getFieldPositionByName(fieldName);
        if (fieldPosition < 0) {
            fieldPosition = Integer.MAX_VALUE;
        }

        return getField(fieldPosition);
    }

    @Override
    public <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass) throws TarantoolValueConverterNotFoundException {
        Optional<TarantoolField> field = getField(fieldPosition);
        return field.isPresent() ? Optional.ofNullable(field.get().getValue(objectClass)) : Optional.empty();
    }

    @Override
    public Iterator<TarantoolField> iterator() {
        return fields.iterator();
    }

    @Override
    public void forEach(Consumer<? super TarantoolField> action) {
        fields.forEach(action);
    }

    @Override
    public Spliterator<TarantoolField> spliterator() {
        return fields.spliterator();
    }

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return mapper.toValue(fields);
    }

    @Override
    public int size() {
        return this.fields.size();
    }

    @Override
    public <V extends Value> void setField(int fieldPosition, V value) {
        if (fieldPosition < 0 ||
                (spaceMetadata != null && fieldPosition >= spaceMetadata.getSpaceFormatMetadata().size())) {
            throw new IndexOutOfBoundsException("Index: " + fieldPosition);
        }

        TarantoolField tarantoolField = value.isNilValue() ? new TarantoolNullField() : new TarantoolFieldImpl<>(value, valueMapper);

        if (fields.size() < fieldPosition) {
            for (int i = fields.size(); i < fieldPosition; i++) {
                fields.add(new TarantoolNullField());
            }
        }

        if (fields.size() == fieldPosition) {
            fields.add(fieldPosition, tarantoolField);
        } else {
            fields.set(fieldPosition, tarantoolField);
        }
    }

    @Override
    public void setField(int fieldPosition, Object value) {
        Value messagePackValue = (value == null) ? ValueFactory.newNil() : objectMapper.toValue(value);
        setField(fieldPosition, messagePackValue);
    }

    @Override
    public void setField(String fieldName, Object value) {
        int fieldPosition = getFieldPositionByName(fieldName);
        if (fieldPosition < 0) {
            throw new TarantoolSpaceFieldNotFoundException(fieldName);
        }

        setField(fieldPosition, value);
    }

    protected int getFieldPositionByName(String fieldName) {
        int fieldPosition = -1;
        if (spaceMetadata != null) {
            fieldPosition = spaceMetadata.getFieldPositionByName(fieldName);
        }

        return fieldPosition;
    }
}
