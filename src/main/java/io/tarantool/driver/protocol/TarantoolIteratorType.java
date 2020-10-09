package io.tarantool.driver.protocol;

/**
 * Represents all types of Tarantool iterators.
 *
 * @author Alexey Kuzin
 */
public enum TarantoolIteratorType {
    ITER_EQ(0x00, "EQ"),
    ITER_REQ(0x01, "REQ"),
    ITER_ALL(0x02, "ALL"),
    ITER_LT(0x03, "LT"),
    ITER_LE(0x04, "LE"),
    ITER_GE(0x05, "GE"),
    ITER_GT(0x06, "GT");
    // TODO support bitset iterators

    private final int code;
    private final String stringCode;

    TarantoolIteratorType(int code, String stringCode) {
        this.code = code;
        this.stringCode = stringCode;
    }

    public int getCode() {
        return code;
    }

    public String getStringCode() {
        return stringCode;
    }

    public TarantoolIteratorType reverse() {
        switch (this) {
            case ITER_GE: return ITER_LE;
            case ITER_GT: return ITER_LT;
            case ITER_LE: return ITER_GE;
            case ITER_LT: return ITER_GT;
            default: return ITER_REQ;
        }
    }

    /**
     * Returns the default iterator type (EQ iterator)
     * @return EQ iterator
     */
    public static TarantoolIteratorType defaultIterator() {
        return ITER_EQ;
    }
}
