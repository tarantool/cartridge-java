package io.tarantool.driver.protocol;

/**
 * All Tarantool request field codes
 *
 * @author Alexey Kuzin
 */
public enum TarantoolRequestFieldType {
    IPROTO_SPACE_ID(0x10),
    IPROTO_INDEX_ID(0x11),
    IPROTO_LIMIT(0x12),
    IPROTO_OFFSET(0x13),
    IPROTO_ITERATOR(0x14),
    IPROTO_KEY(0x20),
    IPROTO_TUPLE(0x21),
    IPROTO_FUNCTION_NAME(0x22),
    IPROTO_EXPRESSION(0x27),
    IPROTO_OPS(0x28);

    private final int code;

    TarantoolRequestFieldType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
