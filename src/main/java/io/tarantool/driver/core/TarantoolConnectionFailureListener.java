package io.tarantool.driver.core;

/**
 * Connection failure listener. Used in {@link TarantoolConnection}
 *
 * @author Alexey Kuzin
 */
public interface TarantoolConnectionFailureListener {
    /**
     * This method is invoked when the connection has been broken. The possible exception can be handled appropriately
     * @param e the disconnection cause
     */
    void onConnectionFailure(Throwable e);
}
