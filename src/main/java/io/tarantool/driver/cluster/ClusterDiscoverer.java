package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.List;

/**
 * Discovery strategy to obtain a list of addresses the cluster nodes.
 *
 * @author Sergey Volgin
 */
public interface ClusterDiscoverer extends AddressProvider, AutoCloseable {

    /**
     * Get list of {@link TarantoolServerAddress}
     *
     * @return list of cluster nodes
     */
    List<TarantoolServerAddress> getNodes();
}
