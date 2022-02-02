package io.tarantool.driver.exceptions.errors;

/**
 * Error codes used to classify errors
 * <a href="https://github.com/tarantool/tarantool/blob/master/src/box/errcode.h">Tarantool error codes</a>
 *
 * @author Oleg Kuznetsov
 */
enum ErrorCode {
    NO_SUCH_PROCEDURE(33L), NO_CONNECTION(77L), TIMEOUT(78L);
    private final Long code;

    ErrorCode(Long code) {
        this.code = code;
    }

    public Long getCode() {
        return code;
    }
}
