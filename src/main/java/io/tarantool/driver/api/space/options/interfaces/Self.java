package io.tarantool.driver.api.space.options.interfaces;


/**
 * @author Artyom Dubinin
 */
public interface Self<T extends Self<T>> {

    T self();
}
