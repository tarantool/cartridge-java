package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;

/**
 * Represent network exception
 *
 * @author Sergey Volgin
 */
public class TarantoolSocketException extends TarantoolClientException {

    private final TarantoolServerAddress tarantoolServerAddress;

    /**
     * @param message the message
     * @param tarantoolServerAddress the address
     * @param cause the cause
     */
    public TarantoolSocketException(String message, TarantoolServerAddress tarantoolServerAddress, Throwable cause) {
        super(message, cause);
        this.tarantoolServerAddress = tarantoolServerAddress;
    }

    /**
     * @param message the message
     * @param tarantoolServerAddress the cause
     */
    public TarantoolSocketException(String message, TarantoolServerAddress tarantoolServerAddress) {
        super(message);
        this.tarantoolServerAddress = tarantoolServerAddress;
    }

    /**
     * Get {@link TarantoolServerAddress} for this exception
     * @return the address
     */
    public TarantoolServerAddress getTarantoolServerAddress() {
        return tarantoolServerAddress;
    }
}
