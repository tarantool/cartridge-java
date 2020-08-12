package io.tarantool.driver.api.tuple;

import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.metadata.TarantoolSpaceMetadata;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.UUID;
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

    private MessagePackMapper mapper;

    /**
     * Constructor for empty tuple
     */
    public TarantoolTupleImpl(MessagePackMapper mapper) {
        this(Collections.emptyList(), mapper, null);
    }

    /**
     * Construct an instance of {@link TarantoolTuple } from a list of objects
     *
     * @param values list of tuple fields data
     * @param mapper provides conversion between MessagePack values and Java objects
     */
    public TarantoolTupleImpl(List<Object> values, MessagePackMapper mapper) {
        this(values, mapper, null);
    }

    /**
     * Construct an instance of {@link TarantoolTuple } from a list of objects. Provides space metadata which adds
     * extra functionality for working with fields and indexes.
     *
     * @param values list of tuple fields data
     * @param mapper provides conversion between MessagePack values and Java objects
     */
    public TarantoolTupleImpl(List<Object> values, MessagePackMapper mapper, TarantoolSpaceMetadata metadata) {
        Assert.notNull(mapper, "MessagePack mapper should not be null");

        this.mapper = mapper;
        this.spaceMetadata = metadata;

        if (values != null) {
            this.fields = new ArrayList<>(values.size());
            for (Object value : values) {
                if (value == null) {
                    this.fields.add(new TarantoolNullField());
                } else {
                    this.fields.add(new TarantoolFieldImpl(value));
                }
            }
        } else {
            this.fields = new ArrayList<>();
        }
    }

    /**
     * Construct an instance of {@link TarantoolTuple }
     *
     * @param value serialized Tarantool tuple
     * @param mapper provides conversion between MessagePack values and Java objects
     */
    public TarantoolTupleImpl(ArrayValue value, MessagePackMapper mapper) {
        this(value, mapper, null);
    }

    /**
     * Basic constructor. Used for converting Tarantool server responses into Java entities.
     * @param value serialized Tarantool tuple
     * @param mapper provides conversion between MessagePack values and Java objects
     * @param spaceMetadata provides field names and other metadata
     */
    public TarantoolTupleImpl(ArrayValue value, MessagePackMapper mapper, TarantoolSpaceMetadata spaceMetadata) {
        Assert.notNull(mapper, "MessagePack mapper should not be null");

        this.mapper = mapper;
        this.spaceMetadata = spaceMetadata;

        if (value != null) {
            this.fields = new ArrayList<>(value.size());
            for (Value fieldValue : value) {
                if (fieldValue.isNilValue()) {
                    fields.add(new TarantoolNullField());
                } else {
                    fields.add(new TarantoolFieldImpl(fieldValue));
                }
            }
        } else {
            this.fields = new ArrayList<>();
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
    public List<TarantoolField> getFields() {
        return fields;
    }

    @Override
    public <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass) {
        Optional<TarantoolField> field = getField(fieldPosition);
        return field.map(tarantoolField -> tarantoolField.getValue(objectClass, mapper));
    }

    @Override
    public <O> Optional<O> getObject(String fieldName, Class<O> objectClass) {
        Optional<TarantoolField> field = getField(fieldName);
        return field.map(tarantoolField -> tarantoolField.getValue(objectClass, mapper));
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
    public void setField(int fieldPosition, TarantoolField field) {
        if (fieldPosition < 0 ||
                (spaceMetadata != null && fieldPosition >= spaceMetadata.getSpaceFormatMetadata().size())) {
            throw new IndexOutOfBoundsException("Index: " + fieldPosition);
        }

        if (field == null) {
            field = new TarantoolNullField();
        }

        if (fields.size() < fieldPosition) {
            for (int i = fields.size(); i < fieldPosition; i++) {
                fields.add(new TarantoolNullField());
            }
        }

        if (fields.size() == fieldPosition) {
            fields.add(fieldPosition, field);
        } else {
            fields.set(fieldPosition, field);
        }
    }

    @Override
    public void setField(String fieldName, TarantoolField field) {
        int fieldPosition = getFieldPositionByName(fieldName);
        if (fieldPosition < 0) {
            throw new TarantoolSpaceFieldNotFoundException(fieldName);
        }

        setField(fieldPosition, field);
    }

    @Override
    public void putObject(int fieldPosition, Object value) {
        TarantoolField tarantoolField = value == null ?
                new TarantoolNullField() : new TarantoolFieldImpl(mapper.toValue(value));

        setField(fieldPosition, tarantoolField);
    }

    @Override
    public void putObject(String fieldName, Object value) {
        TarantoolField tarantoolField = value == null ?
                new TarantoolNullField() : new TarantoolFieldImpl(mapper.toValue(value));

        setField(fieldName, tarantoolField);
    }

    @Nullable
    @Override
    public byte[] getByteArray(int fieldPosition) {
        return getObject(fieldPosition, byte[].class).orElse(null);
    }

    @Nullable
    @Override
    public byte[] getByteArray(String fieldName) {
        return getObject(fieldName, byte[].class).orElse(null);
    }

    @Nullable
    @Override
    public Boolean getBoolean(int fieldPosition) {
        return getObject(fieldPosition, Boolean.class).orElse(null);
    }

    @Nullable
    @Override
    public Boolean getBoolean(String fieldName) {
        return getObject(fieldName, Boolean.class).orElse(null);
    }

    @Nullable
    @Override
    public Double getDouble(int fieldPosition) {
        return getObject(fieldPosition, Double.class).orElse(null);
    }

    @Override
    public Double getDouble(String fieldName) {
        return getObject(fieldName, Double.class).orElse(null);
    }

    @Nullable
    @Override
    public Float getFloat(int fieldPosition) {
        return getObject(fieldPosition, Float.class).orElse(null);
    }

    @Nullable
    @Override
    public Float getFloat(String fieldName) {
        return getObject(fieldName, Float.class).orElse(null);
    }

    @Nullable
    @Override
    public Integer getInteger(int fieldPosition) {
        return getObject(fieldPosition, Integer.class).orElse(null);
    }

    @Nullable
    @Override
    public Integer getInteger(String fieldName) {
        return getObject(fieldName, Integer.class).orElse(null);
    }

    @Nullable
    @Override
    public Long getLong(int fieldPosition) {
        return getObject(fieldPosition, Long.class).orElse(null);
    }

    @Nullable
    @Override
    public Long getLong(String fieldName) {
        return getObject(fieldName, Long.class).orElse(null);
    }

    @Nullable
    @Override
    public String getString(int fieldPosition) {
        return getObject(fieldPosition, String.class).orElse(null);
    }

    @Nullable
    @Override
    public String getString(String fieldName) {
        return getObject(fieldName, String.class).orElse(null);
    }

    @Nullable
    @Override
    public UUID getUUID(int fieldPosition) {
        return getObject(fieldPosition, UUID.class).orElse(null);
    }

    @Nullable
    @Override
    public UUID getUUID(String fieldName) {
        return getObject(fieldName, UUID.class).orElse(null);
    }

    @Nullable
    @Override
    public BigDecimal getDecimal(int fieldPosition) {
        return getObject(fieldPosition, BigDecimal.class).orElse(null);
    }

    @Nullable
    @Override
    public BigDecimal getDecimal(String fieldName) {
        return getObject(fieldName, BigDecimal.class).orElse(null);
    }

    @Nullable
    @Override
    public List getList(int fieldPosition) {
        return getObject(fieldPosition, List.class).orElse(null);
    }

    @Nullable
    @Override
    public List getList(String fieldName) {
        return getObject(fieldName, List.class).orElse(null);
    }

    @Nullable
    @Override
    public Map getMap(int fieldPosition) {
        return getObject(fieldPosition, Map.class).orElse(null);
    }

    @Nullable
    @Override
    public Map getMap(String fieldName) {
        return getObject(fieldName, Map.class).orElse(null);
    }

    protected int getFieldPositionByName(String fieldName) {
        int fieldPosition = -1;
        if (spaceMetadata != null) {
            fieldPosition = spaceMetadata.getFieldPositionByName(fieldName);
        }

        return fieldPosition;
    }
}
