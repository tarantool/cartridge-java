package io.tarantool.driver.api;

import java.util.Collection;

/**
 * Provides a collection of Tarantool server addresses corresponding to the cluster nodes
 *
 * @author Alexey Kuzin
 * @author Sergey Volgin
 */
public interface TarantoolClusterAddressProvider extends AutoCloseable {
    /**
     * The collection of Tarantool server nodes which belong to the same cluster
     *
     * @return collection of {@link TarantoolServerAddress}
     */
    Collection<TarantoolServerAddress> getAddresses();

    /**
     * Specify callback for refreshing connections to addresses.
     * <p>
     * For example: you can run it when you want saying about your Tarantool server addresses is changed
     *
     * @param runnable callback for running refresh connections
     */
    default void setRefreshCallback(Runnable runnable) {
    }

    @Override
    default void close() {
    }
}
