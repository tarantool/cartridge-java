package io.tarantool.driver.exceptions;

import org.msgpack.value.Value;

/**
 * Represents errors occurring when required mapper or converter not found.
 *
 * @author Vladimir Rogach
 */
public class TarantoolMapperConfigurationException extends TarantoolClientException {
    public TarantoolMapperConfigurationException(String message) {
        super(String.format("Failed to find required converter: %s", message));
    }
}
