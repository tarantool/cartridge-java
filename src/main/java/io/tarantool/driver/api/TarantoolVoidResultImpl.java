package io.tarantool.driver.api;

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
