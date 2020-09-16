package io.tarantool.driver.core;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Helper class. Maintains a collection of {@link TarantoolConnectionListener}
 *
 * @author Alexey Kuzin
 */
public class TarantoolConnectionListeners {
    private final List<TarantoolConnectionListener> listeners;

    /**
     * Basic constructor.
     */
    public TarantoolConnectionListeners() {
        this.listeners = new LinkedList<>();
    }

    /**
     * Allows to add several listeners right on instantiation.
     * @param tarantoolConnectionListeners connection listeners
     */
    public TarantoolConnectionListeners(List<TarantoolConnectionListener> tarantoolConnectionListeners) {
        this();
        listeners.addAll(tarantoolConnectionListeners);
    }

    /**
     * Add a single {@link TarantoolConnectionListener} to collection
     * @param listener connection listener
     * @return this instance
     */
    public TarantoolConnectionListeners add(TarantoolConnectionListener listener) {
        listeners.add(listener);
        return this;
    }

    /**
     * Returns all listeners previously added to the collection
     * @return a copy of the listeners collection
     */
    public List<TarantoolConnectionListener> all() {
        return new LinkedList<>(listeners);
    }

    /**
     * Construct a collection from an enumeration of {@link TarantoolConnectionListener}
     * @param listeners connection listeners
     * @return new {@link TarantoolConnectionListeners} instance
     */
    public static TarantoolConnectionListeners of(TarantoolConnectionListener... listeners) {
        return new TarantoolConnectionListeners(Arrays.asList(listeners));
    }
}
