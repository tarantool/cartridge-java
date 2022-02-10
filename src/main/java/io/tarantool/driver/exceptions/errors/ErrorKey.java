package io.tarantool.driver.exceptions.errors;

import org.msgpack.value.StringValue;

/**
 * Representations for error key
 *
 * @author Oleg Kuznetsov
 */
interface ErrorKey {
    String getKey();

    StringValue getMsgPackKey();
}
