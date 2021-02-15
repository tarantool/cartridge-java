package io.tarantool.driver.exceptions;

/**
 * Basic exception returned by Tarantool server for unsuccessful operations.
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
