package io.tarantool.driver.utils;


import java.util.Collection;

/**
 * Provides simple assertions throwing {@link IllegalArgumentException} with the specified message.
 *
 * @author Alexey Kuzin
 */
public final class Assert {
    /**
     * Asserts if the passed expression is {@code true}
     *
     * @param expression returns boolean
     * @param message exception message
     * @throws IllegalArgumentException if the assertion fails
     */
    public static void state(boolean expression, String message) throws IllegalArgumentException {
        if (!expression) {
            throw new IllegalArgumentException(message != null ? message : "");
        }
    }

    /**
     * Asserts if the passed object is not {@code null}
     *
     * @param object nullable object
     * @param message exception message
     * @throws IllegalArgumentException if the assertion fails
     */
    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message != null ? message : "");
        }
    }

    /**
     * Asserts if the given String is not empty
     *
     * @param object nullable String
     * @param message exception message
     * @throws IllegalArgumentException if the assertion fails
     */
    public static void hasText(String object, String message) {
        if (object == null || object.isEmpty()) {
            throw new IllegalArgumentException(message != null ? message : "");
        }
    }

    /**
     * Asserts if the given Collection is not empty
     *
     * @param collection nullable Collection
     * @param message exception message
     * @throws IllegalArgumentException if the assertion fails
     */
    public static void notEmpty(Collection<?> collection, String message) {
        if (collection == null || collection.isEmpty()) {
            throw new IllegalArgumentException(message != null ? message : "");
        }
    }

    private Assert() {
    }
}
