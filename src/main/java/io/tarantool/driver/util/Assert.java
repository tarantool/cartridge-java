package io.tarantool.driver.util;


public class Assert {

    public static void state(boolean expression, String message) {
        if (!expression) {
            throw new IllegalStateException(message);
        }
    }

    public static void hasText(@Nullable String text, String message) {
        if (!StringUtils.hasText(text)) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void notNull(@Nullable Object o, String message) {
        if (o == null) {
            throw new IllegalArgumentException(message);
        }
    }

}
