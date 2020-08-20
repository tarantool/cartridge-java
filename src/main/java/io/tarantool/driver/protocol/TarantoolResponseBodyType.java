package io.tarantool.driver.protocol;

/**
 * Incapsulates Tarantool response body types
 */
public enum TarantoolResponseBodyType {
    EMPTY,
    IPROTO_DATA,
    IPROTO_ERROR,
    IPROTO_SQL;

    public static TarantoolResponseBodyType fromCode(int code) throws TarantoolProtocolException {
        switch (code) {
            case 0x00: return EMPTY;
            case 0x30: return IPROTO_DATA;
            case 0x31: return IPROTO_ERROR;
            case 0x42: return IPROTO_SQL;
            default:
                throw new TarantoolProtocolException("Unsupported Tarantool response body key {}", code);
        }
    }
}
