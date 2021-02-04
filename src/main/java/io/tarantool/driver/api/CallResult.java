package io.tarantool.driver.api;

/**
 * Basic interface for Tarantool call operation result -- an array of elements corresponding to the Lua multi-return
 * result, usually two values -- an result and a error.
 *
 * @param <T> target result type
 * @author Alexey Kuzin
 */
public interface CallResult<T> {
    /**
     * Get multi-return result as a single value
     *
     * @return call result
     */
    T value();
}
