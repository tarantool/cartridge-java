package io.tarantool.driver.mappers;

/**
 * Represents exceptions that occur while performing conversion between Java objects and MessagePack entities
 *
 * @author Alexey Kuzin
 */
public class MessagePackValueMapperException extends RuntimeException {

    public MessagePackValueMapperException() {
        super("Failed to perform object to MessagePack conversion");
    }

    public MessagePackValueMapperException(String s) {
        super(s);
    }

    public MessagePackValueMapperException(String format, Object... values) {
        this(String.format(format, values));
    }
}
