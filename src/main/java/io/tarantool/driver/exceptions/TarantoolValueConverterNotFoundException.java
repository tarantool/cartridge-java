package io.tarantool.driver.exceptions;

/**
 * Represents an error when the supplied value mapper doesn't contain a converter suitable for particular object type
 *
 * @author Alexey Kuzin
 */
public class TarantoolValueConverterNotFoundException extends RuntimeException {
    public TarantoolValueConverterNotFoundException(Class<?> entityClass, Class<?> tupleClass) {
        super(String.format(
                "Value converter for source entity type %s and object type %s not found", entityClass, tupleClass));
    }
}
