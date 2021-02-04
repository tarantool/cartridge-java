package io.tarantool.driver.api;

/**
 * {@link CallResult} with one result value (first item of the multi-return result is treated as value)
 *
 * @param <T> result type
 * @author Alexey Kuzin
 */
public interface SingleValueCallResult<T> extends CallResult<T> {
}
