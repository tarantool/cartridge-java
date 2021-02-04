package io.tarantool.driver.api;

import java.util.List;

/**
 * {@link CallResult} implementation with multi result value (all items item of the multi-return result is treated
 * as value)
 *
 * @param <T> the result content type
 * @param <R> the result type
 * @author Alexey Kuzin
 */
public interface MultiValueCallResult<T, R extends List<T>> extends CallResult<R> {
}
