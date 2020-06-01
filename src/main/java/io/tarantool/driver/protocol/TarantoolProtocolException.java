package io.tarantool.driver.protocol;

/**
 * Represents errors that occur while decoding Tarantool packets
 *
 * @author Alexey Kuzin
 */
public class TarantoolProtocolException extends Exception {

    public TarantoolProtocolException(String s) {
        super("Invalid Tarantool packet: " + s);
    }

    public TarantoolProtocolException(Exception e) {
        super("Invalid Tarantool packet", e);
    }

    public TarantoolProtocolException(String s, Object... values) {
        super("Invalid Tarantool packet: " + String.format(s, values));
    }
}
