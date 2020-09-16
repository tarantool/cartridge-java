package io.tarantool.driver;

import java.util.Collection;

/**
 * Provides a collection of Tarantool server addresses corresponding to the cluster nodes
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public interface TarantoolClusterAddressProvider extends AutoCloseable {
    /**
     * The the collection of Tarantool server nodes which belong to the same cluster
     * @return collection of {@link TarantoolServerAddress}
     */
    Collection<TarantoolServerAddress> getAddresses();

    @Override
    default void close() {
    }
}
