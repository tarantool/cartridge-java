package io.tarantool.driver.cluster;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.auth.TarantoolCredentials;

/**
 * Class-container for {@link TarantoolClusterDiscoverer} configuration
 *
 * @author Sergey Volgin
 */
public class TarantoolClusterDiscoveryEndpoint implements ClusterDiscoveryEndpoint {

    private String entryFunction;
    private TarantoolServerAddress serverAddress;
    private TarantoolCredentials credentials;

    /**
     * Create an instance
     *
     * @see TarantoolClusterDiscoverer
     */
    public TarantoolClusterDiscoveryEndpoint() {

    }

    public void setServerAddress(TarantoolServerAddress serverAddress) {
        this.serverAddress = serverAddress;
    }

    public TarantoolCredentials getCredentials() {
        return credentials;
    }

    public void setCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    public String getEntryFunction() {
        return entryFunction;
    }

    public void setEntryFunction(String entryFunction) {
        this.entryFunction = entryFunction;
    }

    public TarantoolServerAddress getServerAddress() {
        return serverAddress;
    }

    public static class Builder {
        private TarantoolClusterDiscoveryEndpoint endpoint;

        public Builder() {
            this.endpoint = new TarantoolClusterDiscoveryEndpoint();
        }

        public Builder withEntryFunction(String entryFunction) {
            this.endpoint.setEntryFunction(entryFunction);
            return this;
        }

        public Builder withServerAddress(TarantoolServerAddress serverAddress) {
            this.endpoint.setServerAddress(serverAddress);
            return this;
        }

        public Builder withCredentials(TarantoolCredentials credentials) {
            this.endpoint.setCredentials(credentials);
            return this;
        }

        public TarantoolClusterDiscoveryEndpoint build() {
            return endpoint;
        }
    }
}
