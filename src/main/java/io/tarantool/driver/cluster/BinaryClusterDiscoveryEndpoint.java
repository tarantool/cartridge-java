package io.tarantool.driver.cluster;

import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolClusterAddressProvider;
import io.tarantool.driver.utils.Assert;

/**
 * Class-container for {@link BinaryDiscoveryClusterAddressProvider} configuration
 *
 * @author Sergey Volgin
 */
public class BinaryClusterDiscoveryEndpoint implements TarantoolClusterDiscoveryEndpoint {

    private TarantoolClusterAddressProvider endpointProvider;
    private TarantoolClientConfig clientConfig = TarantoolClientConfig.builder().build();
    private String discoveryFunction;

    /**
     * Create an instance
     *
     * @see BinaryDiscoveryClusterAddressProvider
     */
    public BinaryClusterDiscoveryEndpoint() {
    }

    /**
     * Get service discovery endpoint provider
     *
     * @return tarantool server address provider instance
     */
    public TarantoolClusterAddressProvider getEndpointProvider() {
        return endpointProvider;
    }

    /**
     * Set service discovery endpoint provider
     *
     * @param endpointProvider a tarantool address provider instance
     */
    public void setEndpointProvider(TarantoolClusterAddressProvider endpointProvider) {
        this.endpointProvider = endpointProvider;
    }

    /**
     * Get client configuration for connecting to the set of the discovery endpoints
     *
     * @return tarantool client configuration
     */
    public TarantoolClientConfig getClientConfig() {
        return clientConfig;
    }

    /**
     * Set client configuration for connecting to the set of the discovery endpoints. The same configuration will be
     * used for each endpoint.
     *
     * @param clientConfig tarantool client configuration
     */
    public void setClientConfig(TarantoolClientConfig clientConfig) {
        this.clientConfig = clientConfig;
    }

    /**
     * Get discovery function name
     *
     * @return discovery function name
     */
    public String getDiscoveryFunction() {
        return discoveryFunction;
    }

    /**
     * Set discovery function name
     *
     * @param discoveryFunction discovery function name
     */
    public void setDiscoveryFunction(String discoveryFunction) {
        this.discoveryFunction = discoveryFunction;
    }

    /**
     * Builder for {@link BinaryClusterDiscoveryEndpoint}
     *
     * @return new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link BinaryClusterDiscoveryEndpoint}
     */
    public static class Builder {
        private final BinaryClusterDiscoveryEndpoint endpoint;

        /**
         * Basic constructor.
         */
        public Builder() {
            this.endpoint = new BinaryClusterDiscoveryEndpoint();
        }

        /**
         * Specify the function name to invoke in the discovery endpoint for getting the list of nodes. The function
         * should not require any parameters and must be exposed as API function. Also the user which is connecting the
         * endpoint must have the appropriate permission for this function.
         *
         * @param discoveryFunction the function name, should not be null
         * @return this builder instance
         */
        public Builder withEntryFunction(String discoveryFunction) {
            Assert.hasText(discoveryFunction, "The discovery function name should not be null or empty");
            this.endpoint.setDiscoveryFunction(discoveryFunction);
            return this;
        }

        /**
         * Specify address provider for the discovery endpoints
         *
         * @param endpointProvider discovery endpoint address privider, should not be null
         * @return this builder instance
         * @see BinaryClusterDiscoveryEndpoint#setEndpointProvider(TarantoolClusterAddressProvider)
         */
        public Builder withEndpointProvider(TarantoolClusterAddressProvider endpointProvider) {
            Assert.notNull(endpointProvider, "Discovery endpoint address provider should not be null");
            this.endpoint.setEndpointProvider(endpointProvider);
            return this;
        }

        /**
         * Specify the client configuration for connecting to the discovery endpoints. The same configuration will be
         * used for all endpoints
         *
         * @param clientConfig tarantool client configuration
         * @return this builder instance
         */
        public Builder withClientConfig(TarantoolClientConfig clientConfig) {
            Assert.notNull(clientConfig, "Client config should not be null");
            this.endpoint.setClientConfig(clientConfig);
            return this;
        }

        /**
         * Build the discovery endpoint configuration
         *
         * @return {@link BinaryClusterDiscoveryEndpoint} instance
         */
        public BinaryClusterDiscoveryEndpoint build() {
            return endpoint;
        }
    }
}
