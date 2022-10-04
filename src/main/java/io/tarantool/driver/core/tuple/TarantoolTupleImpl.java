package io.tarantool.driver.core.tuple;

import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.TarantoolField;
import io.tarantool.driver.api.tuple.TarantoolNullField;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolSpaceFieldNotFoundException;
import io.tarantool.driver.mappers.MessagePackMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.utils.Assert;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePackException;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private static final long serialVersionUID = 20200708L;

    private final TarantoolSpaceMetadata spaceMetadata;

    private final MessagePackMapper mapper;

    private transient ArrayList<TarantoolField> fields = new ArrayList<>();

    /**
     * Constructor for empty tuple
     *
     * @param mapper provides conversion between MessagePack values and Java objects
     */
    public TarantoolTupleImpl(MessagePackMapper mapper) {
        this(mapper, null);
    }

    /**
     * Constructor for empty tuple with metadata
     *
     * @param mapper provides conversion between MessagePack values and Java objects
     * @param metadata provides information about the target space
     */
    public TarantoolTupleImpl(MessagePackMapper mapper, TarantoolSpaceMetadata metadata) {
        this(Collections.emptyList(), mapper, metadata);
    }

    /**
     * Construct an instance of {@link TarantoolTuple } from a list of objects
     *
     * @param values list of tuple fields data
     * @param mapper provides conversion between MessagePack values and Java objects
     */
    public TarantoolTupleImpl(Collection<?> values, MessagePackMapper mapper) {
        this(values, mapper, null);
    }

    /**
     * Construct an instance of {@link TarantoolTuple } from a list of objects. Provides space metadata which adds
     * extra functionality for working with fields and indexes.
     *
     * @param values list of tuple fields data
     * @param mapper provides conversion between MessagePack values and Java objects
     * @param metadata provides information about the target space
     */
    public TarantoolTupleImpl(Collection<?> values, MessagePackMapper mapper, TarantoolSpaceMetadata metadata) {
        Assert.notNull(mapper, "MessagePack mapper should not be null");

        this.mapper = mapper;
        this.spaceMetadata = metadata;

        if (values != null) {
            this.fields.ensureCapacity(values.size());
            for (Object value : values) {
                if (value == null) {
                    this.fields.add(TarantoolNullField.INSTANCE);
                } else {
                    this.fields.add(new TarantoolFieldImpl(value));
                }
            }
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
            this.fields.ensureCapacity(value.size());
            for (Value fieldValue : value) {
                if (fieldValue.isNilValue()) {
                    fields.add(TarantoolNullField.INSTANCE);
                } else {
                    fields.add(new TarantoolFieldImpl(fieldValue));
                }
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
    public List<TarantoolField> getFields() {
        return fields;
    }

    @Override
    public <O> Optional<O> getObject(int fieldPosition, Class<O> objectClass) {
        Optional<TarantoolField> field = getField(fieldPosition);
        return field.map(tarantoolField -> tarantoolField.getValue(objectClass, mapper));
    }

    @Override
    public boolean canGetObject(int fieldPosition, Class<?> objectClass) {
        Optional<TarantoolField> field = getField(fieldPosition);
        return field.map(tarantoolField -> tarantoolField.canConvertValue(objectClass, mapper)).orElse(false);
    }

    @Override
    public <O> Optional<O> getObject(String fieldName, Class<O> objectClass) {
        Optional<TarantoolField> field = getField(fieldName);
        return field.map(tarantoolField -> tarantoolField.getValue(objectClass, mapper));
    }

    @Override
    public boolean canGetObject(String fieldName, Class<?> objectClass) {
        Optional<TarantoolField> field = getField(fieldName);
        return field.map(tarantoolField -> tarantoolField.canConvertValue(objectClass, mapper)).orElse(false);
    }

    @Override
    public Optional<?> getObject(int fieldPosition) {
        Optional<TarantoolField> field = getField(fieldPosition);
        return field.map(tarantoolField -> tarantoolField.getValue(mapper));
    }

    @Override
    public Optional<?> getObject(String fieldName) {
        Optional<TarantoolField> field = getField(fieldName);
        return field.map(tarantoolField -> tarantoolField.getValue(mapper));
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
            field = TarantoolNullField.INSTANCE;
        }

        if (fields.size() < fieldPosition) {
            for (int i = fields.size(); i < fieldPosition; i++) {
                fields.add(TarantoolNullField.INSTANCE);
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
                TarantoolNullField.INSTANCE : new TarantoolFieldImpl(mapper.toValue(value));

        setField(fieldPosition, tarantoolField);
    }

    @Override
    public void putObject(String fieldName, Object value) {
        TarantoolField tarantoolField = value == null ?
               TarantoolNullField.INSTANCE : new TarantoolFieldImpl(mapper.toValue(value));

        setField(fieldName, tarantoolField);
    }

    @Override
    public byte[] getByteArray(int fieldPosition) {
        return getObject(fieldPosition, byte[].class).orElse(null);
    }

    @Override
    public byte[] getByteArray(String fieldName) {
        return getObject(fieldName, byte[].class).orElse(null);
    }

    @Override
    public Boolean getBoolean(int fieldPosition) {
        return getObject(fieldPosition, Boolean.class).orElse(null);
    }

    @Override
    public Boolean getBoolean(String fieldName) {
        return getObject(fieldName, Boolean.class).orElse(null);
    }

    @Override
    public Double getDouble(int fieldPosition) {
        return getObject(fieldPosition, Double.class).orElse(null);
    }

    @Override
    public Double getDouble(String fieldName) {
        return getObject(fieldName, Double.class).orElse(null);
    }

    @Override
    public Float getFloat(int fieldPosition) {
        return getObject(fieldPosition, Float.class).orElse(null);
    }

    @Override
    public Float getFloat(String fieldName) {
        return getObject(fieldName, Float.class).orElse(null);
    }

    @Override
    public Integer getInteger(int fieldPosition) {
        return getObject(fieldPosition, Integer.class).orElse(null);
    }

    @Override
    public Integer getInteger(String fieldName) {
        return getObject(fieldName, Integer.class).orElse(null);
    }

    @Override
    public Long getLong(int fieldPosition) {
        return getObject(fieldPosition, Long.class).orElse(null);
    }

    @Override
    public Long getLong(String fieldName) {
        return getObject(fieldName, Long.class).orElse(null);
    }

    @Override
    public String getString(int fieldPosition) {
        return getObject(fieldPosition, String.class).orElse(null);
    }

    @Override
    public String getString(String fieldName) {
        return getObject(fieldName, String.class).orElse(null);
    }

    @Override
    public Character getCharacter(int fieldPosition) {
        return getObject(fieldPosition, Character.class).orElse(null);
    }

    @Override
    public Character getCharacter(String fieldName) {
        return getObject(fieldName, Character.class).orElse(null);
    }

    @Override
    public UUID getUUID(int fieldPosition) {
        return getObject(fieldPosition, UUID.class).orElse(null);
    }

    @Override
    public UUID getUUID(String fieldName) {
        return getObject(fieldName, UUID.class).orElse(null);
    }

    @Override
    public BigDecimal getDecimal(int fieldPosition) {
        return getObject(fieldPosition, BigDecimal.class).orElse(null);
    }

    @Override
    public BigDecimal getDecimal(String fieldName) {
        return getObject(fieldName, BigDecimal.class).orElse(null);
    }

    @Override
    public List<?> getList(int fieldPosition) {
        return getObject(fieldPosition, List.class).orElse(null);
    }

    @Override
    public List<?> getList(String fieldName) {
        return getObject(fieldName, List.class).orElse(null);
    }

    @Override
    public Map<?, ?> getMap(int fieldPosition) {
        return getObject(fieldPosition, Map.class).orElse(null);
    }

    @Override
    public Map<?, ?> getMap(String fieldName) {
        return getObject(fieldName, Map.class).orElse(null);
    }

    protected int getFieldPositionByName(String fieldName) {
        int fieldPosition = -1;
        if (spaceMetadata != null) {
            fieldPosition = spaceMetadata.getFieldPositionByName(fieldName);
        }

        return fieldPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TarantoolTupleImpl)) {
            return false;
        }
        TarantoolTupleImpl that = (TarantoolTupleImpl) o;
        return Objects.equals(getFields(), that.getFields());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFields());
    }

    private void writeObject(java.io.ObjectOutputStream out) {
        try {
            out.defaultWriteObject();

            MessagePacker packer = MessagePack.newDefaultPacker(out);
            packer.packValue(toMessagePackValue(mapper));
            packer.flush();
        } catch (IOException | MessagePackException e) {
            throw new TarantoolClientException("Failed to serialize tuple fields", e);
        }
    }

    private void readObject(java.io.ObjectInputStream in) {
        ArrayValue value;
        try {
            in.defaultReadObject();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(in);
            value = unpacker.unpackValue().asArrayValue();
        } catch (IOException | MessagePackException | ClassNotFoundException e) {
            throw new TarantoolClientException("Failed to deserialize tuple fields", e);
        }

        this.fields = new ArrayList<>(value.size());
        for (Value fieldValue : value) {
            if (fieldValue.isNilValue()) {
                fields.add(TarantoolNullField.INSTANCE);
            } else {
                fields.add(new TarantoolFieldImpl(fieldValue));
            }
        }
    }
}
