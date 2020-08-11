package io.tarantool.driver.cluster;

import io.tarantool.driver.ServerAddress;

import java.util.Collection;

/**
 * Strategy for getting the next address to connect to tarantool cluster
 *
 * @author Sergey Volgin
 */
public interface AddressProvider {

    /**
     * Get an current node address
     * @return the {@link ServerAddress}
     */
    ServerAddress getAddress();

    /**
     * Get the next node address
     * @return the {@link ServerAddress}
     */
    ServerAddress getNext();

    /**
     * Update current address list by new list
     *
     * @param addresses list {@link ServerAddress} of cluster nodes
     */
    void updateAddressList(Collection<ServerAddress> addresses);
}
