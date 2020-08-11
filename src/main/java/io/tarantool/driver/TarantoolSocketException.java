package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;

/**
 * Represent network exception
 *
 * @author Sergey Volgin
 */
public class TarantoolSocketException extends TarantoolClientException {

    private final ServerAddress serverAddress;

    /**
     * @param message the message
     * @param serverAddress the address
     * @param cause the cause
     */
    public TarantoolSocketException(String message, ServerAddress serverAddress, Throwable cause) {
        super(message, cause);
        this.serverAddress = serverAddress;
    }

    /**
     * @param message the message
     * @param serverAddress the cause
     */
    public TarantoolSocketException(String message, ServerAddress serverAddress) {
        super(message);
        this.serverAddress = serverAddress;
    }

    /**
     * Get {@link ServerAddress} for this exception
     * @return the address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }
}
