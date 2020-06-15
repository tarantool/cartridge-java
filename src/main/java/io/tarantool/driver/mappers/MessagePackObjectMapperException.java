package io.tarantool.driver.mappers;

/**
 * Represents exceptions that occur while performing conversion between Java objects and MessagePack entities
 *
 * @author Alexey Kuzin
 */
public class MessagePackObjectMapperException extends RuntimeException {

    public MessagePackObjectMapperException() {
        super("Failed to perform object to MessagePack conversion");
    }

    public MessagePackObjectMapperException(String s) {
        super(s);
    }

    public MessagePackObjectMapperException(String format, Object... values) {
        this(String.format(format, values));
    }

    public MessagePackObjectMapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
