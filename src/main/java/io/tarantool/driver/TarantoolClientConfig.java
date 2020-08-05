package io.tarantool.driver;

import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackMapper;

/**
 * Class-container for {@link TarantoolClient} configuration.
 *
 * It is recommended to use the {@link TarantoolClientConfig.Builder} for constructing the configuration
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientConfig {
    private TarantoolCredentials credentials;
    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
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
         * Build a {@link TarantoolClientConfig} instance
         * @return configured instance
         */
        public TarantoolClientConfig build() {
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
