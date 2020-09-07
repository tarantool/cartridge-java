package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;
import org.springframework.util.Assert;

/**
 * Class-container for {@link BinaryDiscoveryClusterAddressProvider} configuration
 *
 * @author Sergey Volgin
 */
public class BinaryClusterDiscoveryEndpoint implements TarantoolClusterDiscoveryEndpoint {

    private TarantoolServerAddress serverAddress;
    private TarantoolCredentials credentials;
    private String discoveryFunction;

    /**
     * Create an instance
     *
     * @see BinaryDiscoveryClusterAddressProvider
     */
    public BinaryClusterDiscoveryEndpoint() {
    }

    /**
     * Get discovery endpoint address
     * @return discovery endpoint address
     */
    public TarantoolServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Set discovery endpoint address
     * @param serverAddress discovery endpoint address
     */
    public void setServerAddress(TarantoolServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    /**
     * Get discovery endpoint address
     * @return discovery endpoint address
     */
    public TarantoolCredentials getCredentials() {
        return credentials;
    }

    /**
     * Set discovery endpoint credentials
     * @param credentials discovery endpoint credentials
     */
    public void setCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Get discovery function name
     * @return discovery function name
     */
    public String getDiscoveryFunction() {
        return discoveryFunction;
    }

    /**
     * Set discovery function name
     * @param discoveryFunction discovery function name
     */
    public void setDiscoveryFunction(String discoveryFunction) {
        this.discoveryFunction = discoveryFunction;
    }

    /**
     * Builder for {@link BinaryClusterDiscoveryEndpoint}
     */
    public static class Builder {
        private BinaryClusterDiscoveryEndpoint endpoint;

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
         * @param discoveryFunction the function name, should not be null
         * @return this builder instance
         */
        public Builder withEntryFunction(String discoveryFunction) {
            Assert.hasText(discoveryFunction, "The discovery function name should not be null or empty");
            this.endpoint.setDiscoveryFunction(discoveryFunction);
            return this;
        }

        /**
         * Specify the discovery endpoint address
         * @param serverAddress Tarantool server address, should not be null
         * @return this builder instance
         */
        public Builder withServerAddress(TarantoolServerAddress serverAddress) {
            Assert.notNull(serverAddress, "Discovery endpoint address should not be null");
            this.endpoint.setServerAddress(serverAddress);
            return this;
        }

        /**
         * Specify the discovery endpoint credentials
         * @param credentials Tarantool server credentials
         * @return this builder instance
         */
        public Builder withCredentials(TarantoolCredentials credentials) {
            this.endpoint.setCredentials(credentials);
            return this;
        }

        /**
         * Build the discovery endpoint configuration
         * @return {@link BinaryClusterDiscoveryEndpoint} instance
         */
        public BinaryClusterDiscoveryEndpoint build() {
            return endpoint;
        }
    }
}
