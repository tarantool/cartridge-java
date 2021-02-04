package io.tarantool.driver.mappers;

/**
 * Represents all types of errors when the target interface parameter type cannot be determined
 *
 * @author Alexey Kuzin
 */
public class InterfaceParameterTypeNotFoundException extends Exception {
    /**
     * Create the exception with a message template
     * @param format template format
     * @param params template parameters
     */
    public InterfaceParameterTypeNotFoundException(String format, Object... params) {
        super(String.format(format, params));
    }
}
