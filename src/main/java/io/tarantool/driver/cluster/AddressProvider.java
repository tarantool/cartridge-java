package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.Collection;

/**
 * Strategy for getting the next address to connect to tarantool cluster
 *
 * @author Sergey Volgin
 */
public interface AddressProvider {

    /**
     * Get an current node address
     * @return the {@link TarantoolServerAddress}
     */
    TarantoolServerAddress getCurrentAddress();

    /**
     * Get the next node address
     * @return the {@link TarantoolServerAddress}
     */
    TarantoolServerAddress getNextAddress();

    /**
     * Update current address list by new list
     *
     * @param addresses list {@link TarantoolServerAddress} of cluster nodes
     */
    void updateAddressList(Collection<TarantoolServerAddress> addresses);
}
