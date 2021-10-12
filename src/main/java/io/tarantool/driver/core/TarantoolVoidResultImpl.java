package io.tarantool.driver.core;

import io.tarantool.driver.api.TarantoolVoidResult;

/**
 * Implementation of Tarantool void result
 *
 * @author Oleg Kuznetsov
 * @author Ivan Dneprov
 */
public class TarantoolVoidResultImpl implements TarantoolVoidResult {

    @Override
    public Void value() {
        return null;
    }
}
