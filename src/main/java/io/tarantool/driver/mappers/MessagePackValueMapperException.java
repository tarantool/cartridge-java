package io.tarantool.driver.mappers;

/**
 * Represents exceptions that occur while performing conversion between MessagePack entities and Java objects
 *
 * @author Alexey Kuzin
 */
public class MessagePackValueMapperException extends RuntimeException {

    public MessagePackValueMapperException() {
        super("Failed to perform MessagePack to object conversion");
    }

    public MessagePackValueMapperException(String s) {
        super(s);
    }

    public MessagePackValueMapperException(String format, Object... values) {
        this(String.format(format, values));
    }

    public MessagePackValueMapperException(String message, Throwable cause) {
        super(message, cause);
    }
}
