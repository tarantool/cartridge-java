package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolTupleConversionException;
import io.tarantool.driver.mappers.converters.ValueConverter;
import org.msgpack.core.MessageTypeCastException;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Basic TarantoolResult implementation. Supports two types of the tuple result: a MessagePack array of arrays
 * (as in IPROTO) and a map with format {@code {"rows": [[], ...], "metadata": []}} as returned by tarantool/crud module
 *
 * @param <T> target result tuple type
 * @author Alexey Kuzin
 */
public class TarantoolResultImpl<T> implements TarantoolResult<T> {

    private static final StringValue RESULT_META = ValueFactory.newString("metadata");
    private static final StringValue RESULT_ROWS = ValueFactory.newString("rows");

    private List<T> tuples;

    public TarantoolResultImpl(Value value, ValueConverter<ArrayValue, T> tupleConverter) {
        if (value.isArrayValue()) {
            // [[[],...]]
            setTuples(value.asArrayValue(), tupleConverter);
        } else if (value.isMapValue()) {
            // [{"metadata" : [...], "rows": [...]}]
            Map<Value, Value> tupleMap = value.asMapValue().map();
            if (!hasRowsAndMetadata(tupleMap)) {
                throw new TarantoolClientException("The received tuple map has wrong format, " +
                    "expected {\"metadata\" : [...], \"rows\": [...]}, got %s", value.toString());
            }
            Value tupleArray = tupleMap.get(RESULT_ROWS);
            if (!tupleArray.isArrayValue()) {
                throw new TarantoolClientException("The \"rows\" field must contain a MessagePack array");
            }
            setTuples(tupleArray.asArrayValue(), tupleConverter);
        } else if (value.isNilValue()) {
            // [nil]
            this.tuples = new ArrayList<>();
        } else {
            throw new TarantoolClientException("The received result cannot be converted to an array of tuples: %s",
                value.toString());
        }
    }

    private void setTuples(ArrayValue tupleArray, ValueConverter<ArrayValue, T> tupleConverter) {
        this.tuples = tupleArray.list().stream()
            .map(v -> {
                try {
                    return tupleConverter.fromValue(v.asArrayValue());
                } catch (MessageTypeCastException e) {
                    throw new TarantoolTupleConversionException(v, e);
                }
            })
            .collect(Collectors.toList());
    }

    private static boolean hasRowsAndMetadata(Map<Value, Value> valueMap) {
        return valueMap.containsKey(RESULT_META) && valueMap.containsKey(RESULT_ROWS);
    }

    @Override
    public int size() {
        return this.tuples.size();
    }

    @Override
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return this.tuples.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return this.tuples.iterator();
    }

    @Override
    public Object[] toArray() {
        return this.tuples.toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return this.tuples.toArray(a);
    }

    @Override
    public boolean add(T t) {
        return this.tuples.add(t);
    }

    @Override
    public boolean remove(Object o) {
        return this.tuples.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return this.tuples.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        return this.tuples.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        return this.tuples.addAll(index, c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return this.tuples.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return this.tuples.retainAll(c);
    }

    @Override
    public void clear() {
        this.tuples.clear();
    }

    @Override
    public T get(int index) {
        return this.tuples.get(index);
    }

    @Override
    public T set(int index, T element) {
        return this.tuples.set(index, element);
    }

    @Override
    public void add(int index, T element) {
        this.tuples.add(index, element);
    }

    @Override
    public T remove(int index) {
        return this.tuples.remove(index);
    }

    @Override
    public int indexOf(Object o) {
        return this.tuples.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return this.tuples.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return this.tuples.listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return this.tuples.listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return this.tuples.subList(fromIndex, toIndex);
    }
}
