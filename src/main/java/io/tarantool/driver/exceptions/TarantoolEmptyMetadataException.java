package io.tarantool.driver.exceptions;

/**
 * The exception occurs when no metadata returned by proxy function when expected something
 *
 * @author Artyom Dubinin
 */
public class TarantoolEmptyMetadataException extends TarantoolClientException {
    private static final String message = "No space metadata returned";

    public TarantoolEmptyMetadataException() {
        super(message);
    }
}
