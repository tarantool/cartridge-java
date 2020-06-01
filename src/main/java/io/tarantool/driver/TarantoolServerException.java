package io.tarantool.driver;

/**
 * Basic exception returned by Tarantool server for unsuccessful operations.
 */
public class TarantoolServerException extends Throwable {

    private Long errorCode;
    private String errorMessage;

    public TarantoolServerException(Long errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public Long getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return String.format("TarantoolServerException: code=%d, message=%s", errorCode, errorMessage);
    }
}
