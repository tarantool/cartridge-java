package io.tarantool.driver.api.space.options;


/**
 * @author Artyom Dubinin
 */
public interface Self<T extends Self<T>> {

    T self();
}
