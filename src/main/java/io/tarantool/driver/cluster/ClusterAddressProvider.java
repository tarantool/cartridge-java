package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.List;

/**
 * Address provider aware of a list of the Tarantool cluster nodes.
 *
 * @author Sergey Volgin
 */
public interface ClusterAddressProvider extends AddressProvider, AutoCloseable {

    /**
     * Get list of {@link TarantoolServerAddress}
     *
     * @return list of cluster nodes
     */
    List<TarantoolServerAddress> getNodes();
}
