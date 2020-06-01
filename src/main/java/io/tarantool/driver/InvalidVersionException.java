package io.tarantool.driver;

/**
 * Occurs when the version received from Tarantool server is invalid or unsupported
 */
public class InvalidVersionException extends Exception {
    public InvalidVersionException() {
        super("Invalid or unsupported Tarantool version");
    }

    public InvalidVersionException(String version) {
        super("Invalid or unsupported Tarantool version: " + (version == null ? "null" : version));
    }

    public InvalidVersionException(Exception e) {
        super("Failed to determine Tarantool version", e);
    }
}
