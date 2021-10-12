package io.tarantool.driver.api;

import io.tarantool.driver.core.TarantoolVoidResultImpl;

/**
 * Shortcut for {@link SingleValueCallResult} with void result
 *
 * @author Oleg Kuznetsov
 * @author Ivan Dneprov
 */
public interface TarantoolVoidResult extends CallResult<Void> {
    TarantoolVoidResult INSTANCE = new TarantoolVoidResultImpl();
}
