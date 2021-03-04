package io.tarantool.driver.core;

/**
 * Connection close listener. Used in {@link TarantoolConnection}
 *
 * @author Alexey Kuzin
 */
public interface TarantoolConnectionCloseListener {
    /**
     * This method is invoked when the connection has been closed. The internal channel may probably be in invalid state
     * @param connection    connection that was disconnected
     */
    void onConnectionClosed(TarantoolConnection connection);
}
