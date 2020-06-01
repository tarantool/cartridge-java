package io.tarantool.driver.protocol;

/**
 * Incapsulates the type codes of a Tarantool response
 *
 * @author Alexey Kuzin
 */
public enum TarantoolResponseType {
    IPROTO_OK,
    IPROTO_NOT_OK;

    public static TarantoolResponseType fromCode(long code) throws TarantoolProtocolException {
        if (code == 0x00) {
            return IPROTO_OK;
        } else if (code >= 0x8000) {
            return IPROTO_NOT_OK;
        } else {
            throw new TarantoolProtocolException("Unknown response code: {}", code);
        }
    }
}
