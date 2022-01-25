package io.tarantool.driver.exceptions.errors;

import io.tarantool.driver.exceptions.TarantoolException;
import io.tarantool.driver.exceptions.TarantoolInternalException;
import io.tarantool.driver.exceptions.TarantoolInternalNetworkException;
import io.tarantool.driver.exceptions.TarantoolNoSuchProcedureException;
import io.tarantool.driver.protocol.TarantoolErrorResult;
import org.msgpack.value.Value;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class-container for built-in tarantool errors factories
 *
 * @author Artyom Dubinin
 * @author Oleg Kuznetsov
 */
public class TarantoolErrors {

    private static final String CLIENT_ERROR_TYPE = "ClientError";

    /**
     * Produces {@link TarantoolInternalException} subclasses
     * from the serialized representation in the format of <code>require('errors').new_class("NewError")</code>,
     *
     * @see <a href="https://github.com/tarantool/errors">tarantool/errors</a>
     */
    public static class TarantoolErrorsErrorFactory implements TarantoolErrorFactory {
        private static final Pattern NETWORK_ERROR_PATTERN = Pattern.compile(
                "(?=.*\"type\":\"" + CLIENT_ERROR_TYPE + "\")"
                        + "(?=.*\"code\":"
                        + "[" + ErrorCode.NO_CONNECTION.getCode() + "|" + ErrorCode.TIMEOUT.getCode() + "])",
                Pattern.DOTALL);

        public TarantoolErrorsErrorFactory() {
        }

        @Override
        public Optional<TarantoolException> create(Value error) {
            if (error == null || !error.isMapValue()) {
                return Optional.empty();
            }

            final Map<Value, Value> errorMap = error.asMapValue().map();

            if (!isErrorsError(errorMap)) {
                return Optional.empty();
            }

            String exceptionMessage =
                    new ErrorMessageBuilder("InnerErrorMessage:", ErrorsErrorKey.values(), errorMap).build();

            if (isNetworkError(errorMap)) {
                return Optional.of(new TarantoolInternalNetworkException(exceptionMessage));
            }

            return Optional.of(new TarantoolInternalException(exceptionMessage));
        }

        /**
         * Check code from tarantool/errors is network code or not
         *
         * @param errorMap map which contain info about error from tarantool/errors
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(Map<Value, Value> errorMap) {
            String err = errorMap.containsKey(ErrorsErrorKey.ERR.getMsgPackKey()) ?
                    errorMap.get(ErrorsErrorKey.ERR.getMsgPackKey()).toString() : null;

            if (err == null) {
                return false;
            }

            // 77 or 78 code
            // FIXME: Blocked by https://github.com/tarantool/crud/issues/186
            Matcher matcher = NETWORK_ERROR_PATTERN.matcher(err);
            return matcher.find();
        }

        /**
         * Check the error message contains field of str,
         * which contains an error in tarantool/errors
         *
         * @param errorMap string message from tarantool/errors
         * @return an {@link Boolean}
         */
        private Boolean isErrorsError(Map<Value, Value> errorMap) {
            return errorMap.containsKey(ErrorsErrorKey.ERROR_MESSAGE.getMsgPackKey());
        }
    }

    /**
     * Produces {@link TarantoolInternalException} subclasses from the serialized representation
     * in the format of <code>box.error:unpack</code>,
     *
     * @see <a href="https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/error/">box_error</a>
     */
    public static class TarantoolBoxErrorFactory implements TarantoolErrorFactory {
        private static final int ERROR_INDICATOR_OFFSET = 32768;

        public TarantoolBoxErrorFactory() {
        }

        @Override
        public Optional<TarantoolException> create(Value error) {
            if (error == null || !error.isMapValue()) {
                return Optional.empty();
            }
            Map<Value, Value> errorMap = error.asMapValue().map();

            if (!isBoxError(errorMap)) {
                return Optional.empty();
            }

            String exceptionMessage =
                    new ErrorMessageBuilder("InnerErrorMessage:", BoxErrorKey.values(), errorMap).build();

            if (isNetworkError(errorMap)) {
                return Optional.of(new TarantoolInternalNetworkException(exceptionMessage));
            }
            return Optional.of(new TarantoolInternalException(exceptionMessage));
        }

        public TarantoolException create(TarantoolErrorResult error) {
            Long code = error.getErrorCode() - ERROR_INDICATOR_OFFSET;
            String message = error.getErrorMessage();

            StringBuilder sb = new StringBuilder("InnerErrorMessage:");
            sb.append("\n").append(BoxErrorKey.CODE.getKey()).append(": ").append(code);
            if (message != null) {
                sb.append("\n").append(BoxErrorKey.MESSAGE.getKey()).append(": ").append(message);
            }
            String exceptionMessage = sb.toString();

            if (isNetworkError(code)) {
                return new TarantoolInternalNetworkException(exceptionMessage);
            }

            if (ErrorCode.NO_SUCH_PROCEDURE.getCode().equals(code)) {
                return new TarantoolNoSuchProcedureException(exceptionMessage);
            }

            return new TarantoolInternalException(exceptionMessage);
        }

        /**
         * Check code from box.error is network code or not
         *
         * @param errorMap map which contain info about error
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(Map<Value, Value> errorMap) {
            long code = errorMap.get(BoxErrorKey.CODE.getMsgPackKey()).asIntegerValue().asLong();
            String type = errorMap.get(BoxErrorKey.TYPE.getMsgPackKey()).toString();

            // 77 or 78 code
            return (code == ErrorCode.NO_CONNECTION.getCode() || code == ErrorCode.TIMEOUT.getCode())
                    && type != null && type.equals(CLIENT_ERROR_TYPE);
        }

        /**
         * Check code from box.error is network code or not
         *
         * @param code code from box.error
         * @return an {@link Boolean}
         */
        private Boolean isNetworkError(Long code) {
            // 77 or 78 code
            return code.equals(ErrorCode.NO_CONNECTION.getCode())
                    || code.equals(ErrorCode.TIMEOUT.getCode());
        }

        /**
         * Check the error message contains fields of code and message
         *
         * @param errorMap map which contain info about error
         * @return an {@link Boolean}
         */
        private Boolean isBoxError(Map<Value, Value> errorMap) {
            return errorMap.containsKey(BoxErrorKey.CODE.getMsgPackKey())
                    && errorMap.containsKey(BoxErrorKey.MESSAGE.getMsgPackKey());
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
