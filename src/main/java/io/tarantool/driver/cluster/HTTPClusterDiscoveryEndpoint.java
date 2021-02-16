package io.tarantool.driver.cluster;

import io.tarantool.driver.utils.Assert;

/**
 * Class-container for {@link HTTPDiscoveryClusterAddressProvider} configuration
 *
 * @author Sergey Volgin
 */
public class HTTPClusterDiscoveryEndpoint implements TarantoolClusterDiscoveryEndpoint {

    private String uri;
    private int connectTimeout = 1000; // milliseconds
    private int readTimeout = 1000; // milliseconds

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
     * Get cluster discovery endpoint connection timeout
     * @return connection timeout, in milliseconds
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set cluster discovery endpoint connection timeout
     * @param connectTimeout connection timeout, in milliseconds
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get response timeout for cluster discovery request
     * @return request timeout, in milliseconds
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Set response timeout for cluster discovery request
     * @param readTimeout request timeout, in milliseconds
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
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
         * Specify the connection timeout for discovery endpoint
         * @param connectTimeout connection timeout, in milliseconds
         * @return this builder instance
         * @see HTTPClusterDiscoveryEndpoint#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            if (connectTimeout <= 0) {
                throw new IllegalArgumentException("Connect timeout must be greater or equal than 0 ms");
            }
            this.endpoint.setConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Specify the read timeout for discovery endpoint connection
         * @param readTimeout timeout of receiving response in the connection, in milliseconds
         * @return this builder instance
         * @see HTTPClusterDiscoveryEndpoint#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            if (readTimeout <= 0) {
                throw new IllegalArgumentException("Read timeout must be greater or equal than 0 ms");
            }
            this.endpoint.setReadTimeout(readTimeout);
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
