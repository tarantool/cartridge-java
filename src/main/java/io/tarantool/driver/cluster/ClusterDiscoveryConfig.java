package io.tarantool.driver.cluster;

/**
 * Class-container for {@link ClusterDiscoverer} configuration.
 * <p>
 * It is recommended to use the {@link ClusterDiscoveryConfig.Builder} for constructing the configuration
 *
 * @author Sergey Volgin
 */
public final class ClusterDiscoveryConfig {

    private ClusterDiscoveryEndpoint endpoint;
    private int serviceDiscoveryDelay = 60_000;
    private int connectTimeout = 1000;
    private int readTimeout = 1000;

    /**
     * Get config of service discovery endpoint
     *
     * @return a {@link ClusterDiscoveryEndpoint} instance
     */
    public ClusterDiscoveryEndpoint getEndpoint() {
        return endpoint;
    }

    /**
     * Set service discovery endpoint config and enable cluster connection
     *
     * @param endpoint a {@link ClusterDiscoveryEndpoint} instance
     */
    public void setEndpoint(ClusterDiscoveryEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    /**
     * Get cluster discovery delay
     *
     * @return cluster discovery delay, milliseconds
     */
    public int getServiceDiscoveryDelay() {
        return serviceDiscoveryDelay;
    }

    /**
     * Set scan period (in milliseconds) of receiving a new list of instances
     *
     * @param serviceDiscoveryDelay period of receiving a new list of instances
     */
    public void setServiceDiscoveryDelay(int serviceDiscoveryDelay) {
        this.serviceDiscoveryDelay = serviceDiscoveryDelay;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Create a builder instance.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for {@link ClusterDiscoveryConfig}
     */
    public static class Builder {

        private ClusterDiscoveryConfig config;

        public Builder() {
            this.config = new ClusterDiscoveryConfig();
        }

        /**
         * Specify scan period of receiving a new list of instances
         *
         * @param delay period of receiving a new list of instances, in milliseconds
         * @return builder
         * @see ClusterDiscoveryConfig#setServiceDiscoveryDelay(int)
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
         *
         * @param endpoint discovery endpoint config
         * @return builder
         * @see ClusterDiscoveryConfig#setEndpoint(ClusterDiscoveryEndpoint)
         */
        public Builder withEndpoint(ClusterDiscoveryEndpoint endpoint) {
            this.config.setEndpoint(endpoint);
            return this;
        }

        public Builder withConnectTimeout(int connectTimeout) {
            if (connectTimeout <= 0) {
                throw new IllegalArgumentException("Connect timeout must be greater than 0 ms");
            }
            this.config.setConnectTimeout(connectTimeout);
            return this;
        }

        public Builder withReadTimeout(int readTimeout) {
            if (readTimeout <= 0) {
                throw new IllegalArgumentException("Read timeout must be greater than 0 ms");
            }
            this.config.setReadTimeout(readTimeout);
            return this;
        }

        /**
         * Build a {@link ClusterDiscoveryConfig} instance
         *
         * @return configured instance
         */
        public ClusterDiscoveryConfig build() {
            return config;
        }
    }
}
