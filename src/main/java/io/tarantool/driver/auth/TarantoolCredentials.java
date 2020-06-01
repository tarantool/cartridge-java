package io.tarantool.driver.auth;

/**
 * Container for different forms of credentials, aware of storing the necessary data and authentication mechanisms
 *
 * @author Alexey Kuzin
 */
public interface TarantoolCredentials {
    /**
     * Return the username to authenticate with its identity
     * @return not empty username
     */
    String getUsername();
}
