package io.tarantool.driver;

import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.mappers.DefaultMessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackObjectMapper;

/**
 * Class-container for {@link StandaloneTarantoolClient} settings
 *
 * @author Alexey Kuzin
 */
public class TarantoolClientConfig {
    private TarantoolCredentials credentials;
    private int connectTimeout;
    private int readTimeout;
    private int requestTimeout;
    private MessagePackObjectMapper mapper = DefaultMessagePackObjectMapper.getInstance();

    public TarantoolClientConfig() {
    }

    /**
     * Get Tarantool credentials store
     * @return
     * @see TarantoolCredentials
     */
    public TarantoolCredentials getCredentials() {
        return credentials;
    }

    /**
     * Set Tarantool credentials store
     * @return
     * @see TarantoolCredentials
     */
    public void setCredentials(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Get TCP connection timeout, in milliseconds
     * @return
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Set TCP connection timeout, in milliseconds
     * @return
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Get request completion timeout, in milliseconds
     * @return
     */
    public int getRequestTimeout() {
        return requestTimeout;
    }

    /**
     * Set request completion timeout, in milliseconds
     * @return
     */
    public TarantoolClientConfig setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    /**
     * Get socket read timeout, in milliseconds
     * @return
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Set socket read timeout, in milliseconds
     * @return
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }


    /**
     * Set mapper for MessagePack entity to object conversion
     * @param mapper a {@link MessagePackObjectMapper} instance
     */
    public void setMapper(MessagePackObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Get MessagePack entity-to-object mapper
     * @return a {@link MessagePackObjectMapper} instance
     */
    public MessagePackObjectMapper getMapper() {
        return mapper;
    }

    /**
     * A builder for {@link TarantoolClientConfig}
     *
     * @author ALexey Kuzin
     */
    public static final class Builder {

        private TarantoolClientConfig config;

        public Builder() {
            config = new TarantoolClientConfig();
        }

        /**
         * (non-Javadoc)
         * @see TarantoolClientConfig#setCredentials(TarantoolCredentials)
         */
        public Builder withCredentials(TarantoolCredentials credentials) {
            config.setCredentials(credentials);
            return this;
        }

        /**
         * (non-Javadoc)
         * @see TarantoolClientConfig#setReadTimeout(int)
         */
        public Builder withReadTimeout(int readTimeout) {
            config.readTimeout = readTimeout;
            return this;
        }

        /**
         * (non-Javadoc)
         * @see TarantoolClientConfig#setConnectTimeout(int)
         */
        public Builder withConnectTimeout(int connectTimeout) {
            config.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * (non-Javadoc
         * @see TarantoolClientConfig#setRequestTimeout(int)
         */
        public Builder withRequestTimeout(int requestTimeout) {
            config.requestTimeout = requestTimeout;
            return this;
        }

        /**
         * Build a {@link TarantoolClientConfig} instance
         * @return
         */
        public TarantoolClientConfig build() {
            return config;
        }

        /**
         * Prepare the builder for new configuration process
         * @return
         */
        public Builder clear() {
            config = new TarantoolClientConfig();
            return this;
        }
    }
}
