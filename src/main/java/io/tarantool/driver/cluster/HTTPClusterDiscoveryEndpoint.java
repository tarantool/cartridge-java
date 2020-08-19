package io.tarantool.driver.cluster;

/**
 * Class-container for {@link HTTPClusterAddressProvider} configuration
 *
 * @author Sergey Volgin
 */
public class HTTPClusterDiscoveryEndpoint implements ClusterDiscoveryEndpoint {

    private final String uri;

    public HTTPClusterDiscoveryEndpoint(String uri) {
        this.uri = uri;
    }

    public String getUri() {
        return uri;
    }
}
