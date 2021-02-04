package io.tarantool.driver.mappers;

/**
 * Represents all types of errors when the target interface parameter class cannot be determined
 *
 * @author Alexey Kuzin
 */
public class InterfaceParameterClassNotFoundException extends Exception {
    /**
     * Create the exception with a cause
     *
     * @param cause usually {@link ClassNotFoundException}
     */
    public InterfaceParameterClassNotFoundException(Throwable cause) {
        super(cause.getMessage());
    }

    /**
     * Create the exception with a message template
     *
     * @param format template format
     * @param params template parameters
     */
    public InterfaceParameterClassNotFoundException(String format, Object... params) {
        super(String.format(format, params));
    }
}
