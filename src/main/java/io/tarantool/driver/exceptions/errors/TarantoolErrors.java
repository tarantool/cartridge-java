package io.tarantool.driver.exceptions.errors;

import io.tarantool.driver.exceptions.TarantoolException;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;
import java.util.Optional;

/**
 * Class-container for built-in tarantool errors factories
 *
 * @author Artyom Dubinin
 */
public class TarantoolErrors {

    /**
     * Error codes used to classify errors
     */
    private enum ErrorsCodes {
        NO_CONNECTION("77"), TIMEOUT("78");
        private final String code;

        ErrorsCodes(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }

    private static final String CLIENT_ERROR = "ClientError";

    /**
     * Produces {@link TarantoolInternalException} subclasses
     * from the serialized representation in the format of <code>require('errors').new_class("NewError")</code>,
     *
     * @see <a href="https://github.com/tarantool/errors">tarantool/errors</a>
     */
    public static class TarantoolErrorsErrorFactory implements TarantoolErrorFactory {
        private static final StringValue LINE = ValueFactory.newString("line");
        private static final StringValue CLASS_NAME = ValueFactory.newString("class_name");
        private static final StringValue ERR = ValueFactory.newString("err");
        private static final StringValue FILE = ValueFactory.newString("file");
        private static final StringValue ERROR_MESSAGE = ValueFactory.newString("str");
        private static final StringValue STACKTRACE = ValueFactory.newString("stack");
        private static final Pattern NETWORK_ERROR_PATTERN = Pattern.compile(
                "(?=.*\"type\":\"" + CLIENT_ERROR + "\")"
                        + "(?=.*\"code\":"
                        + "[" + ErrorsCodes.NO_CONNECTION.getCode() + "|" + ErrorsCodes.TIMEOUT.getCode() + "])",
                Pattern.DOTALL);

        public TarantoolErrorsErrorFactory() {
        }

        @Override
        public Optional<TarantoolException> create(Value error) {
            if (error == null || !error.isMapValue()) {
                return Optional.empty();
            }
            Map<Value, Value> map = error.asMapValue().map();

            String exceptionMessage = "";
            String errorMessage = map.containsKey(ERROR_MESSAGE) ? map.get(ERROR_MESSAGE).toString() : null;
            String err = map.containsKey(ERR) ? map.get(ERR).toString() : null;
            String line = map.containsKey(LINE) ? map.get(LINE).toString() : null;
            String className = map.containsKey(CLASS_NAME) ? map.get(CLASS_NAME).toString() : null;
            String file = map.containsKey(FILE) ? map.get(FILE).toString() : null;
            String stacktrace = map.containsKey(STACKTRACE) ? map.get(STACKTRACE).toString() : null;

            StringBuilder sb = new StringBuilder("InnerErrorMessage:");
            if (errorMessage != null) {
                sb.append("\n").append(ERROR_MESSAGE).append(": ").append(errorMessage);
            }
            if (line != null) {
                sb.append("\n").append(LINE).append(": ").append(line);
            }
            if (className != null) {
                sb.append("\n").append(CLASS_NAME).append(": ").append(className);
            }
            if (err != null) {
                sb.append("\n").append(ERR).append(": ").append(file);
            }
            if (stacktrace != null) {
                sb.append("\n").append(STACKTRACE).append(": ").append(stacktrace);
            }
            exceptionMessage = sb.toString();

            if (!isErrorsError(errorMessage)) {
                return Optional.empty();
            }

            if (isNetworkError(err)) {
                return Optional.of(new TarantoolInternalNetworkException(exceptionMessage));
            }
            return Optional.of(new TarantoolInternalException(exceptionMessage));
        }

        /**
         * Check code from tarantool/errors is network code or not
         *
         * @param err error message from tarantool/errors
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(String err) {
            // 77 or 78 code
            // FIXME: Blocked by https://github.com/tarantool/crud/issues/186
            if (err == null) {
                return false;
            }
            Matcher matcher = NETWORK_ERROR_PATTERN.matcher(err);
            return matcher.find();
        }

        /**
         * Check the error message contains field of str,
         * which contains an error in tarantool/errors
         *
         * @param errorMessage string message from tarantool/errors
         * @return an {@link Boolean}
         */
        private Boolean isErrorsError(String errorMessage) {
            return errorMessage != null;
        }
    }

    /**
     * Produces {@link TarantoolInternalException} subclasses from the serialized representation
     * in the format of <code>box.error:unpack</code>,
     *
     * @see <a href="https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/error/">box_error</a>
     */
    public static class TarantoolBoxErrorFactory implements TarantoolErrorFactory {
        private static final StringValue CODE = ValueFactory.newString("code");
        private static final StringValue BASE_TYPE = ValueFactory.newString("base_type");
        private static final StringValue TYPE = ValueFactory.newString("type");
        private static final StringValue MESSAGE = ValueFactory.newString("message");
        private static final StringValue TRACE = ValueFactory.newString("trace");
        private static final Long ERROR_INDICATOR_OFFSET = 32768L;

        public TarantoolBoxErrorFactory() {
        }

        @Override
        public Optional<TarantoolException> create(Value error) {
            if (error == null || !error.isMapValue()) {
                return Optional.empty();
            }
            Map<Value, Value> map = error.asMapValue().map();

            String exceptionMessage = "";
            String code = map.containsKey(CODE) ? map.get(CODE).toString() : null;
            String message = map.containsKey(MESSAGE) ? map.get(MESSAGE).toString() : null;
            String type = map.containsKey(TYPE) ? map.get(TYPE).toString() : null;
            String baseType = map.containsKey(BASE_TYPE) ? map.get(BASE_TYPE).toString() : null;
            String trace = map.containsKey(TRACE) ? map.get(TRACE).toString() : null;

            StringBuilder sb = new StringBuilder("InnerErrorMessage:");
            if (code != null) {
                sb.append("\n").append(CODE).append(": ").append(code);
            }
            if (message != null) {
                sb.append("\n").append(MESSAGE).append(": ").append(message);
            }
            if (baseType != null) {
                sb.append("\n").append(BASE_TYPE).append(": ").append(baseType);
            }
            if (type != null) {
                sb.append("\n").append(TYPE).append(": ").append(type);
            }
            if (trace != null) {
                sb.append("\n").append(TRACE).append(": ").append(trace);
            }
            exceptionMessage = sb.toString();

            if (!isBoxError(code, message)) {
                return Optional.empty();
            }

            if (isNetworkError(code, type)) {
                return Optional.of(new TarantoolInternalNetworkException(exceptionMessage));
            }
            return Optional.of(new TarantoolInternalException(exceptionMessage));
        }

        public TarantoolException create(TarantoolErrorResult error) {
            Long code = error.getErrorCode() - ERROR_INDICATOR_OFFSET;
            String message = error.getErrorMessage();

            StringBuilder sb = new StringBuilder("InnerErrorMessage:");
            sb.append("\n").append(CODE).append(": ").append(code);
            if (message != null) {
                sb.append("\n").append(MESSAGE).append(": ").append(message);
            }
            String exceptionMessage = sb.toString();

            if (isNetworkError(code.toString())) {
                return new TarantoolInternalNetworkException(exceptionMessage);
            }
            return new TarantoolInternalException(exceptionMessage);
        }

        /**
         * Check code from box.error is network code or not
         *
         * @param code code from box.error
         * @param type type from box.error
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(String code, String type) {
            // 77 or 78 code
            return (code.equals(ErrorsCodes.NO_CONNECTION.getCode()) ||
                    code.equals(ErrorsCodes.TIMEOUT.getCode())) &&
                    type != null &&
                    type.equals(CLIENT_ERROR);
        }

        /**
         * Check code from box.error is network code or not
         *
         * @param code code from box.error
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(String code) {
            // 77 or 78 code
            return code.equals(ErrorsCodes.NO_CONNECTION.getCode()) ||
                    code.equals(ErrorsCodes.TIMEOUT.getCode());
        }

        /**
         * Check the error message contains fields of code and message
         *
         * @param code    code from box.error
         * @param message string message from box.error
         * @return an {@link Boolean}
         */
        private Boolean isBoxError(String code, String message) {
            return code != null && message != null;
        }
    }

    /**
     * The factory is finalizing, i.e. errors passed into
     * it will always be introverted as appropriate for the given factory
     * The error is generated in a message that is passed to {@link TarantoolInternalException}
     */
    public static class TarantoolUnrecognizedErrorFactory implements TarantoolErrorFactory {
        public TarantoolUnrecognizedErrorFactory() {
        }

        @Override
        public Optional<TarantoolException> create(Value error) {
            String exceptionMessage = String.valueOf(error);
            return Optional.of(new TarantoolInternalException(exceptionMessage));
        }
    }
}
