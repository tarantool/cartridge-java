package io.tarantool.driver.cluster;

/**
 * Class-container for {@link TarantoolClusterDiscoverer} configuration
 *
 * @author Sergey Volgin
 */
public class TarantoolClusterDiscoveryEndpoint implements ClusterDiscoveryEndpoint {

    private final String entryFunction;

    /**
     * Create an instance
     *
     * @param entryFunction name of stored lua function than return list of addresses cluster nodes
     * @see TarantoolClusterDiscoverer
     */
    public TarantoolClusterDiscoveryEndpoint(String entryFunction) {
        this.entryFunction = entryFunction;
    }

    public String getEntryFunction() {
        return entryFunction;
    }
}
