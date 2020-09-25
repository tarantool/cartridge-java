package io.tarantool.driver.cluster;

import org.springframework.util.Assert;

/**
 * Class-container for {@link HTTPDiscoveryClusterAddressProvider} configuration
 *
 * @author Sergey Volgin
 */
public class HTTPClusterDiscoveryEndpoint implements TarantoolClusterDiscoveryEndpoint {

    private String uri;

    /**
     * Create an instance
     */
    public HTTPClusterDiscoveryEndpoint() {
    }

    /**
     * Create an instance, specifying URI for connection
     * @param uri discovery endpoint URI
     */
    public HTTPClusterDiscoveryEndpoint(String uri) {
        this.uri = uri;
    }

    /**
     * Get discovery endpoint URI
     * @return discovery endpoint URI
     */
    public String getUri() {
        return uri;
    }

    /**
     * Set discovery endpoint URI
     * @param uri discovery endpoint URI
     */
    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * Builder for {@link HTTPClusterDiscoveryEndpoint}
     */
    public static class Builder {
        private HTTPClusterDiscoveryEndpoint endpoint;

        /**
         * Basic constructor.
         */
        public Builder() {
            this.endpoint = new HTTPClusterDiscoveryEndpoint();
        }

        /**
         * Specify the cluster discovery endpoint URI
         * @param uri discovery endpoint URI, should not be null or empty
         * @return this builder instance
         */
        public Builder withURI(String uri) {
            Assert.hasText(uri, "The discovery endpoint URI should not be null or empty");
            this.endpoint.setUri(uri);
            return this;
        }

        /**
         * Build the discovery endpoint configuration
         * @return a {@link HTTPClusterDiscoveryEndpoint} instance
         */
        public HTTPClusterDiscoveryEndpoint build() {
            return endpoint;
        }
    }
}
