package io.tarantool.driver.exceptions.errors;

import io.tarantool.driver.exceptions.TarantoolException;
import org.msgpack.value.Value;

import java.util.Optional;

/**
 * A factory that accepts an error and returns a {@link TarantoolException}
 * if this error matches the format of the factory.
 * This is essentially a factory and a chain of responsibility.
 *
 * @author Artyom Dubinin
 */
public interface TarantoolErrorFactory {
    /**
     * Parse the serialized error object and if it matches any of the familiar formats,
     * produce and return a corresponding exception, or an empty {@link Optional} otherwise.
     *
     * @param error error received from Tarantool
     * @return an {@link Optional}, If the format is appropriate, then Optional will not be null
     */
    Optional<TarantoolException> create(Value error);
}
