package io.tarantool.driver.core;

import java.util.Collection;

/**
 * Special cycling iterator for Tarantool connections
 *
 * @author Alexey Kuzin
 */
public class TarantoolConnectionIterator extends CyclingIterator<TarantoolConnection> {

    /**
     * Basic constructor.
     *
     * @param connections collection to iterate over
     */
    public TarantoolConnectionIterator(Collection<TarantoolConnection> connections) {
        super(connections);
    }
}
