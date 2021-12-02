package io.tarantool.driver.core.connection;

/**
 * Represents connection sequence states
 *
 * @author Alexey Kuzin
 */
public enum ConnectionMode {
    /**
     * Block all requests before starting the init sequence. Enabled on start and when no connections
     * are available
     */
    FULL,
    /**
     * Do not block requests until the init sequence completes. Enabled when some connections are still alive
     */
    PARTIAL,
    /**
     * Init sequence completed, requests will not block
     */
    OFF,

    IN_PROGRESS;
}
