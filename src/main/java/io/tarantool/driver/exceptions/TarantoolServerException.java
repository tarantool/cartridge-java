package io.tarantool.driver.exceptions;

/**
 * Represents exception returned by Tarantool server for unsuccessful operations on the protocol level
 *
 * @author Alexey Kuzin
 */
public class TarantoolServerException extends TarantoolException {

    private Long errorCode;
    private String errorMessage;

    public TarantoolServerException(Long errorCode, String errorMessage) {
        super(String.format("TarantoolServerException: code=%d, message=%s", errorCode, errorMessage));
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public Long getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
