package io.tarantool.driver;

import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.cluster.SimpleAddressProvider;
import io.tarantool.driver.cluster.ClusterDiscoveryConfig;
import io.tarantool.driver.cluster.RoundRobinAddressProvider;
import io.tarantool.driver.cluster.SingleAddressProvider;
import io.tarantool.driver.cluster.TarantoolClusterDiscoveryEndpoint;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Class-container for {@link TarantoolClient} configuration.
 *
 * It is recommended to use the {@link TarantoolClientConfig.Builder} for constructing the configuration
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientConfig {
    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "password";

    private TarantoolCredentials credentials;
    private int connectTimeout = 1000;
    private int readTimeout = 1000;
    private int requestTimeout = 2000;
    private ClusterDiscoveryConfig clusterDiscoveryConfig;
    private SimpleAddressProvider simpleAddressProvider;
    private List<TarantoolServerAddress> hosts;
    private MessagePackMapper messagePackMapper =
            DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();

    /**
     * Basic constructor.
     */
    public TarantoolClientConfig() {
    }

    /**
     * Get Tarantool credentials
     * @return configured Tarantool user credentials
     * @see TarantoolCredentials
     */
    public TarantoolCredentials getCredentials() {
        return credentials;
    }

    /**
     * Set Tarantool credentials store
     * @param credentials Tarantool user credentials
     * @see TarantoolCredentials
     */
    public void setCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Get TCP connection timeout, in milliseconds
     * @return a number
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set TCP connection timeout, in milliseconds
     * @param connectTimeout timeout for establishing a connection to Tarantool server
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get request completion timeout, in milliseconds
     * @return a number
     */
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Set request completion timeout, in milliseconds
     * @param requestTimeout timeout for receiving the response for a request to Tarantool server
     */
    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * Get socket read timeout, in milliseconds
     * @return a number
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Set socket read timeout, in milliseconds
     * @param readTimeout timeout for reading data from a socket, in milliseconds
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Get mapper between Java objects and MessagePack entities
     * @return a {@link MessagePackMapper} instance
     */
    public MessagePackMapper getMessagePackMapper() {
        return messagePackMapper;
    }

    /**
     * Set mapper between Java objects and MessagePack entities
     * @param messagePackMapper {@link MessagePackMapper} instance
     */
    public void setMessagePackMapper(MessagePackMapper messagePackMapper) {
        this.messagePackMapper = messagePackMapper;
    }

    /**
     * Get strategy for selecting server
     * @return a {@link SimpleAddressProvider}
     */
    public SimpleAddressProvider getSimpleAddressProvider() {
        return simpleAddressProvider;
    }

    /**
     * Set strategy for selecting server
     * @param simpleAddressProvider a {@link SimpleAddressProvider} instance
     */
    public void setSimpleAddressProvider(SimpleAddressProvider simpleAddressProvider) {
        this.simpleAddressProvider = simpleAddressProvider;
    }

    /**
     * Get list of tarantool hosts to use when connection to tarantool server or cluster
     * @return list of {@link TarantoolServerAddress} addresses
     */
    public List<TarantoolServerAddress> getHosts() {
        return hosts;
    }

    /**
     * Set list of tarantool hosts to use when connection to tarantool cluster
     * @param hosts list of {@link TarantoolServerAddress} addresses
     */
    public void setHosts(List<TarantoolServerAddress> hosts) {
        this.hosts = hosts;
    }

    /**
     * Get config for cluster discovery
     * @return a {@link ClusterDiscoveryConfig}
     */
    public ClusterDiscoveryConfig getClusterDiscoveryConfig() {
        return clusterDiscoveryConfig;
    }

    /**
     * Set config for cluster discovery
     * @param clusterDiscoveryConfig a {@link ClusterDiscoveryConfig} instance
     */
    public void setClusterDiscoveryConfig(ClusterDiscoveryConfig clusterDiscoveryConfig) {
        this.clusterDiscoveryConfig = clusterDiscoveryConfig;
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
     * A builder for {@link TarantoolClientConfig}
     */
    public static final class Builder {

        private TarantoolClientConfig config;

        /**
         * Basic constructor.
         */
        public Builder() {
            config = new TarantoolClientConfig();
        }

        /**
         * Specify user credentials
         * @param credentials the Tarantool user credentials
         * @return builder
         * @see TarantoolClientConfig#setCredentials(TarantoolCredentials)
         */
        public Builder withCredentials(TarantoolCredentials credentials) {
            config.setCredentials(credentials);
            return this;
        }

        /**
         * Specify read timeout
         * @param readTimeout the timeout for reading the responses from Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            config.setReadTimeout(readTimeout);
            return this;
        }

        /**
         * Specify connection timeout
         * @param connectTimeout the timeout for connecting to the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            config.setConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Specify request timeout
         * @param requestTimeout the timeout for receiving a response from the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setRequestTimeout(int)
         */
        public Builder withRequestTimeout(int requestTimeout) {
            config.setRequestTimeout(requestTimeout);
            return this;
        }

        /**
         * Specify mapper between Java objects and MessagePack entities
         * @param mapper configured {@link MessagePackMapper} instance
         * @return builder
         * @see TarantoolClientConfig#setMessagePackMapper(MessagePackMapper)
         */
        public Builder withMessagePackMapper(MessagePackMapper mapper) {
            config.setMessagePackMapper(mapper);
            return this;
        }

        /**
         * Specify strategy for selecting server
         * @param addressProvider a {@link SimpleAddressProvider}, which the server selection strategy
         * @return builder
         * @see TarantoolClientConfig#setSimpleAddressProvider(SimpleAddressProvider)
         */
        public Builder withAddressProvider(SimpleAddressProvider addressProvider) {
            config.setSimpleAddressProvider(addressProvider);
            return this;
        }

        /**
         * Specify cluster discovery config
         * @param clusterDiscoveryConfig a {@link ClusterDiscoveryConfig} instance
         * @return builder
         * @see TarantoolClientConfig#setClusterDiscoveryConfig(ClusterDiscoveryConfig)
         */
        public Builder withClusterDiscoveryConfig(ClusterDiscoveryConfig clusterDiscoveryConfig) {
            config.setClusterDiscoveryConfig(clusterDiscoveryConfig);
            return this;
        }

        /**
         * Specify tarantool hosts addresses. A duplicate server addresses are removed from the list.
         * @param hosts the initial host list
         * @return builder
         * @see TarantoolClientConfig#setHosts(List)
         */
        public Builder withHosts(List<TarantoolServerAddress> hosts) {
            Assert.notNull(hosts, "Hosts list must not be null");
            if (hosts.isEmpty()) {
                throw new IllegalArgumentException("hosts list may not be empty");
            }
            if (config.getHosts() != null) {
                throw new IllegalArgumentException("The host list is already set");
            }

            Set<TarantoolServerAddress> hostsSet = new LinkedHashSet<>(hosts.size());
            for (TarantoolServerAddress tarantoolServerAddress : hosts) {
                Assert.notNull(tarantoolServerAddress, "TarantoolServerAddress must not be null");
                hostsSet.add(new TarantoolServerAddress(tarantoolServerAddress.getHost(),
                        tarantoolServerAddress.getPort()));
            }

            config.setHosts(new ArrayList<>(hostsSet));
            return this;
        }

        /**
         * Specify tarantool server address
         * @param host tarantool address
         * @param port tarantool port
         * @return builder
         */
        public Builder withHost(String host, int port) {
            if (config.getHosts() != null) {
                throw new IllegalArgumentException("The host list is already set");
            }
            config.setHosts(Collections.singletonList(new TarantoolServerAddress(host, port)));
            return this;
        }

        /**
         * Build a {@link TarantoolClientConfig} instance
         * @return configured instance
         */
        public TarantoolClientConfig build() {
            if (config.getHosts() == null) {
                config.setHosts(Collections.singletonList(new TarantoolServerAddress()));
            }

            if (config.getCredentials() == null) {
                config.setCredentials(new SimpleTarantoolCredentials(DEFAULT_USER, DEFAULT_PASSWORD));
            }

            if (config.getSimpleAddressProvider() == null) {
                if (config.getHosts().size() == 1 && config.getClusterDiscoveryConfig() == null) {
                    config.setSimpleAddressProvider(new SingleAddressProvider(config.getHosts().get(0)));
                } else {
                    config.setSimpleAddressProvider(new RoundRobinAddressProvider(config.getHosts()));
                }
            }

            final ClusterDiscoveryConfig clusterConfig = config.getClusterDiscoveryConfig();
            if (clusterConfig != null && clusterConfig.getEndpoint() instanceof TarantoolClusterDiscoveryEndpoint &&
                    ((TarantoolClusterDiscoveryEndpoint) clusterConfig.getEndpoint()).getCredentials() == null) {
                ((TarantoolClusterDiscoveryEndpoint) clusterConfig.getEndpoint())
                        .setCredentials(config.getCredentials());
            }

            return config;
        }

        /**
         * Prepare the builder for new configuration process
         * @return the empty builder
         */
        public Builder clear() {
            config = new TarantoolClientConfig();
            return this;
        }
    }
}
