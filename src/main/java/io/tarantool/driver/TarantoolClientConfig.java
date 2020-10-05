package io.tarantool.driver;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;
import org.springframework.util.Assert;

/**
 * Class-container for {@link TarantoolClient} configuration.
 *
 * It is recommended to use the {@link TarantoolClientConfig.Builder} for constructing the configuration
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientConfig {

    private static final int DEFAULT_CONNECT_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_READ_TIMEOUT = 1000; // milliseconds
    private static final int DEFAULT_REQUEST_TIMEOUT = 2000; // milliseconds
    private static final int DEFAULT_CONNECTIONS = 1;

    private TarantoolCredentials credentials;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int readTimeout = DEFAULT_READ_TIMEOUT;
    private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private int connections = DEFAULT_CONNECTIONS;
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
     * Get number of connections to be established with the target server. Default value is 1
     * @return number of server connections
     */
    public int getConnections() {
        return connections;
    }

    /**
     * Set number of connections to be established with the target server
     * @param connections number of server connections
     */
    public void setConnections(int connections) {
        this.connections = connections;
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
            Assert.notNull(credentials, "Tarantool server credentials should not be null");
            config.setCredentials(credentials);
            return this;
        }

        /**
         * Specify response reading timeout. Default is 1000 milliseconds
         * @param readTimeout the timeout for reading the responses from Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            Assert.state(readTimeout > 0, "Response reading timeout should be greater than 0");
            config.setReadTimeout(readTimeout);
            return this;
        }

        /**
         * Specify connection timeout. Default is 1000 milliseconds
         * @param connectTimeout the timeout for connecting to the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            Assert.state(connectTimeout > 0, "Connection timeout should be greater than 0");
            config.setConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Specify request timeout. Default is 2000 milliseconds
         * @param requestTimeout the timeout for receiving a response from the Tarantool server, in milliseconds
         * @return builder
         * @see TarantoolClientConfig#setRequestTimeout(int)
         */
        public Builder withRequestTimeout(int requestTimeout) {
            Assert.state(requestTimeout > 0, "Request timeout should be greater than 0");
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
            Assert.notNull(mapper, "MessagePack mapper should not be null");
            config.setMessagePackMapper(mapper);
            return this;
        }

        /**
         * Specify the number of connections used for sending requests to the server. The default value is 1.
         * More connections may help if a request can stuck on the server side or if the request payloads are big.
         * @param connections the number of connections
         * @return builder
         */
        public Builder withConnections(int connections) {
            Assert.state(connections > 1, "The number of server connections must be greater than 0");
            config.connections = connections;
            return this;
        }

        /**
         * Build a {@link TarantoolClientConfig} instance
         * @return configured instance
         */
        public TarantoolClientConfig build() {
            if (config.getCredentials() == null) {
                config.setCredentials(new SimpleTarantoolCredentials());
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
