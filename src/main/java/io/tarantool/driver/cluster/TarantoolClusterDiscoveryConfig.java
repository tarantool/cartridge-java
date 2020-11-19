package io.tarantool.driver.cluster;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.utils.Assert;

/**
 * Class-container for service discovery configuration.
 * <p>
 * It is recommended to use the {@link TarantoolClusterDiscoveryConfig.Builder} for constructing the configuration
 *
 * @author Sergey Volgin
 */
public final class TarantoolClusterDiscoveryConfig {

    private TarantoolClusterDiscoveryEndpoint endpoint;
    private int serviceDiscoveryDelay = 60_000; // milliseconds
    private int connectTimeout = 1000; // milliseconds
    private int readTimeout = 1000; // milliseconds

    /**
     * Get config of service discovery endpoint
     * @return a {@link TarantoolClusterDiscoveryEndpoint} instance
     */
    public TarantoolClusterDiscoveryEndpoint getEndpoint() {
        return endpoint;
    }

    /**
     * Set service discovery endpoint config and enable cluster connection
     * @param endpoint a {@link TarantoolClusterDiscoveryEndpoint} instance
     */
    public void setEndpoint(TarantoolClusterDiscoveryEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Get cluster discovery delay
     * @return cluster discovery delay, milliseconds
     */
    public int getServiceDiscoveryDelay() {
        return serviceDiscoveryDelay;
    }

    /**
     * Set scan period (in milliseconds) of receiving a new list of instances
     * @param serviceDiscoveryDelay period of receiving a new list of instances
     */
    public void setServiceDiscoveryDelay(int serviceDiscoveryDelay) {
        this.serviceDiscoveryDelay = serviceDiscoveryDelay;
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
     * Create a builder instance.
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link TarantoolClusterDiscoveryConfig}
     */
    public static class Builder {

        private TarantoolClusterDiscoveryConfig config;

        public Builder() {
            this.config = new TarantoolClusterDiscoveryConfig();
        }

        /**
         * Specify scan period of receiving a new list of instances
         * @param delay period of receiving a new list of instances, in milliseconds
         * @return this builder instance
         * @see TarantoolClusterDiscoveryConfig#setServiceDiscoveryDelay(int)
         */
        public Builder withDelay(int delay) {
            if (delay <= 0) {
                throw new IllegalArgumentException("Discovery delay must be greater than 0 ms");
            }
            this.config.setServiceDiscoveryDelay(delay);
            return this;
        }

        /**
         * Specify service discovery config and enable using service discovery
         * @param endpoint discovery endpoint config, should not be null
         * @return this builder instance
         * @see TarantoolClusterDiscoveryConfig#setEndpoint(TarantoolClusterDiscoveryEndpoint)
         */
        public Builder withEndpoint(TarantoolClusterDiscoveryEndpoint endpoint) {
            Assert.notNull(endpoint, "Cluster discovery endpoint config should not be null");
            this.config.setEndpoint(endpoint);
            return this;
        }

        /**
         * Specify the connection timeout for discovery endpoint
         * @param connectTimeout connection timeout, in milliseconds
         * @return this builder instance
         * @see TarantoolClusterDiscoveryConfig#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            if (connectTimeout <= 0) {
                throw new IllegalArgumentException("Connect timeout must be greater or equal than 0 ms");
            }
            this.config.setConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Specify the read timeout for discovery endpoint connection
         * @param readTimeout timeout of receiving response in the connection, in milliseconds
         * @return this builder instance
         * @see TarantoolClusterDiscoveryConfig#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            if (readTimeout <= 0) {
                throw new IllegalArgumentException("Read timeout must be greater or equal than 0 ms");
            }
            this.config.setReadTimeout(readTimeout);
            return this;
        }

        /**
         * Build a {@link TarantoolClusterDiscoveryConfig} instance
         * @return configured instance
         */
        public TarantoolClusterDiscoveryConfig build() {
            if (config.getEndpoint() == null) {
                throw new TarantoolClientException("Cluster discovery endpoint must be not null");
            }
            return config;
        }
    }
}
