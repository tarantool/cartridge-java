package io.tarantool.driver.protocol;

/**
 * Incapsulates the type codes of a Tarantool response
 *
 * @author Alexey Kuzin
 */
public enum TarantoolResponseType {
    IPROTO_OK,
    IPROTO_INSERT,
    IPROTO_REPLACE,
    IPROTO_UPDATE,
    IPROTO_DELETE,
    IPROTO_UPSERT,
    IPROTO_NOT_OK;

    public static TarantoolResponseType fromCode(long code) throws TarantoolProtocolException {
        if (code == 0x00) {
            return IPROTO_OK;
        } else if (code == 0x02) {
            return IPROTO_INSERT;
        } else if (code == 0x03) {
            return IPROTO_REPLACE;
        } else if (code == 0x04) {
            return IPROTO_UPDATE;
        } else if (code == 0x05) {
            return IPROTO_DELETE;
        } else if (code == 0x09) {
            return IPROTO_UPSERT;
        } else if (code >= 0x8000) {
            return IPROTO_NOT_OK;
        } else {
            throw new TarantoolProtocolException("Unknown response code: {}", code);
        }
    }
}
