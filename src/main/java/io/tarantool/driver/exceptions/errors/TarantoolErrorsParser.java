package io.tarantool.driver.exceptions.errors;

import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.exceptions.TarantoolException;
import org.msgpack.core.MessagePackException;
import org.msgpack.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A parser that pushes the error into different types of tarantool errors
 * and looks at which one fits, if none fits, then at the end there will be a stub error.
 * This is essentially the implementation of the Chain of Command pattern.
 * As a result, an exception of the desired type and with the desired message is thrown.
 *
 * @author Artyom Dubinin
 */
public final class TarantoolErrorsParser {
    private static final List<TarantoolErrorFactory> errorsFactories = Arrays.asList(
        new TarantoolErrors.TarantoolBoxErrorFactory(),
        new TarantoolErrors.TarantoolErrorsErrorFactory()
    );
    private static final TarantoolErrorFactory defaultFactory = new TarantoolErrors.TarantoolUnrecognizedErrorFactory();

    private TarantoolErrorsParser() {
    }

    /**
     * Parse the error from tarantool
     *
     * @param error error received from Tarantool
     * @return an exception of the special type and with the special message is thrown
     * @throws TarantoolClientException if the error message conversion cannot be performed
     */
    public static TarantoolException parse(Value error) {
        try {
            Optional<TarantoolException> exception;
            for (TarantoolErrorFactory errorFactory : errorsFactories) {
                exception = errorFactory.create(error);
                if (exception.isPresent()) {
                    return exception.get();
                }
            }
            return defaultFactory.create(error).get();
        } catch (MessagePackException e) {
            throw new TarantoolClientException("Failed to unpack internal error", e);
        }
    }
}
