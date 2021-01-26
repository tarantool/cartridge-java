package io.tarantool.driver.exceptions;

import org.msgpack.value.Value;

/**
 * Represents errors occurring when MessagePack mapper tries to parse the incoming data into a tuple object
 *
 * @author Alexey Kuzin
 */
public class TarantoolTupleConversionException extends TarantoolClientException {
    public TarantoolTupleConversionException(Value messagePackValue, Throwable cause) {
        super(String.format("Failed to convert MessagePack array %s to tuple", messagePackValue.toString()), cause);
    }
}
