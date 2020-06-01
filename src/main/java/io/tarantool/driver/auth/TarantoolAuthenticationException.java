package io.tarantool.driver.auth;

import io.tarantool.driver.StandaloneTarantoolClient;

/**
 * This exception is thrown when the {@link StandaloneTarantoolClient} fails to authenticate with given data
 *
 * @author Alexey Kuzin
 */
public class TarantoolAuthenticationException extends Exception {
    public TarantoolAuthenticationException() {
        super("Failed to authenticate to the Tarantool server");
    }

    public TarantoolAuthenticationException(Throwable e) {
        super("Failed to authenticate to the Tarantool server", e);
    }
}
