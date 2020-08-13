package io.tarantool.driver.protocol.operations;

/**
 * Encapsulates a set of supported Tarantool tuple operators
 * for {@link io.tarantool.driver.protocol.requests.TarantoolUpdateRequest} and
 * {@link io.tarantool.driver.protocol.requests.TarantoolUpsertRequest}
 *
 * @author Sergey Volgin
 */
public enum TarantoolOperationType {

    ADD('+'),
    BITWISEAND('&'),
    BITWISEOR('|'),
    BITWISEXOR('^'),
    DELETE('#'),
    INSERT('!'),
    SET('='),
    SPLICE(':'),
    SUBTRACT('-'),
    ;

    private final Character code;

    TarantoolOperationType(Character code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return code.toString();
    }
}
