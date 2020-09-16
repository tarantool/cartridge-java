package io.tarantool.driver.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This iterator is lock-free and loops infinitely over a collection.
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public class CyclingIterator<T> implements Iterator<T> {

    private final List<T> items;
    private final int size;
    private final AtomicInteger position = new AtomicInteger(0);

    /**
     * Basic constructor.
     * @param items collection to iterate over
     */
    public CyclingIterator(Collection<T> items) {
        this.items = Collections.synchronizedList(new ArrayList<>(items));
        this.size = items.size();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public T next() {
        return items.get(position.getAndUpdate(p -> (p + 1) % size));
    }
}
