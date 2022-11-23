package io.tarantool.driver.api.conditions;

import io.tarantool.driver.protocol.TarantoolIteratorType;

/**
 * Filtering condition operator. See <a href="https://github.com/tarantool/crud#select">
 * https://github.com/tarantool/crud#select</a>.
 *
 * @author Alexey Kuzin
 */
public enum Operator {
    EQ("="),
    LT("<"),
    LE("<="),
    GT(">"),
    GE(">=");

    private final String code;

    Operator(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public TarantoolIteratorType toIteratorType() {
        switch (this) {
            case LT:
                return TarantoolIteratorType.ITER_LT;
            case LE:
                return TarantoolIteratorType.ITER_LE;
            case GT:
                return TarantoolIteratorType.ITER_GT;
            case GE:
                return TarantoolIteratorType.ITER_GE;
            default:
                return TarantoolIteratorType.ITER_EQ;
        }
    }

    @Override
    public String toString() {
        return "Operator{" +
            "opCode=" + code +
            '}';
    }
}
