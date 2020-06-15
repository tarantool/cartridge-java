package io.tarantool.driver;

import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;

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
    private MessagePackObjectMapper objectMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();
    private MessagePackValueMapper valueMapper = DefaultMessagePackMapperFactory.getInstance().defaultComplexTypesMapper();

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
     * Get object-to-MessagePack entity mapper
     * @return a {@link MessagePackObjectMapper} instance
     */
    public MessagePackObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * Set mapper for object to MessagePack entity conversion
     * @param objectMapper a {@link MessagePackObjectMapper} instance
     */
    public void setObjectMapper(MessagePackObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Get MessagePack entity-to-object mapper
     * @return a {@link MessagePackObjectMapper} instance
     */
    public MessagePackValueMapper getValueMapper() {
        return valueMapper;
    }

    /**
     * Set valueMapper for MessagePack entity to object conversion
     * @param valueMapper a {@link MessagePackValueMapper} instance
     */
    public void setValueMapper(MessagePackValueMapper valueMapper) {
        this.valueMapper = valueMapper;
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
         * Specify object-to-MessagePack entity mapper
         * @param mapper configured {@link MessagePackObjectMapper} instance
         * @return builder
         * @see TarantoolClientConfig#setObjectMapper(MessagePackObjectMapper)
         */
        public Builder withObjectMapper(MessagePackObjectMapper mapper) {
            config.setObjectMapper(mapper);
            return this;
        }

        /**
         * Specify MessagePack entity-to-object mapper
         * @param mapper configured {@link MessagePackValueMapper} instance
         * @return builder
         * @see TarantoolClientConfig#setValueMapper(MessagePackValueMapper)
         */
        public Builder withValueMapper(MessagePackValueMapper mapper) {
            config.setValueMapper(mapper);
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
