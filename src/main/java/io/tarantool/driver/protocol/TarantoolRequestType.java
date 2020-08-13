package io.tarantool.driver.protocol;

/**
 * Encapsulates a set of supported Tarantool request codes
 *
 * @author Alexey Kuzin
 */
public enum TarantoolRequestType {

    IPROTO_SELECT(0x01),
    IPROTO_INSERT(0x02),
    IPROTO_REPLACE(0x03),
    IPROTO_UPDATE(0x04),
    IPROTO_DELETE(0x05),
    IPROTO_AUTH(0x07),
    IPROTO_UPSERT(0x09),
    IPROTO_CALL(0x0a);

    private long code;

    TarantoolRequestType(long code) {
        this.code = code;
    }

    public long getCode() {
        return code;
    }
}
