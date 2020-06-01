package io.tarantool.driver.protocol;

/**
 * Represents all types of Tarantool iterators.
 *
 * @author Alexey Kuzin
 */
public enum TarantoolIteratorType {
    ITER_EQ (0x00),
    ITER_REQ(0x01),
    ITER_ALL(0x02),
    ITER_LT (0x03),
    ITER_LE (0x04),
    ITER_GE (0x05),
    ITER_GT (0x06);
    // TODO support bitset iterators

    private int code;

    TarantoolIteratorType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    /**
     * Returns the default iterator type (EQ iterator)
     * @return EQ iterator
     */
    public static TarantoolIteratorType defaultIterator() {
        return ITER_EQ;
    }
}
