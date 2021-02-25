package io.tarantool.driver.core;

/**
 * Connection failure listener. Used in {@link TarantoolConnection}
 *
 * @author Alexey Kuzin
 */
public interface TarantoolConnectionFailureListener {
    /**
     * This method is invoked when the connection has been broken. The possible exception can be handled appropriately
     * @param connection    connection that was disconnected
     * @param e             disconnection cause
     */
    void onConnectionFailure(TarantoolConnection connection, Throwable e);
}
