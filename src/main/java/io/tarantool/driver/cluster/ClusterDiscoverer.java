package io.tarantool.driver.cluster;

import io.tarantool.driver.ServerAddress;

import java.util.List;

/**
 * Discovery strategy to obtain a list of addresses the cluster nodes.
 *
 * @author Sergey Volgin
 */
public interface ClusterDiscoverer extends AutoCloseable {

    /**
     * Get list of {@link ServerAddress}
     *
     * @return list of cluster nodes
     */
    List<ServerAddress> getNodes();
}
