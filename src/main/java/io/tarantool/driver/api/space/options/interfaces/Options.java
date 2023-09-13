package io.tarantool.driver.api.space.options.interfaces;

import io.tarantool.driver.api.space.options.enums.ProxyOption;

import java.util.Optional;

/**
 * Marker interface for space operations options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface Options {

    /**
     * Add named option.
     *
     * @param option name of option
     * @param value  value of option
     */
    void addOption(ProxyOption option, Object value);

    /**
     * Return option value by name.
     *
     * @param option      option name
     * @param optionClass option value class
     * @param <T>         option value type
     * @return option value
     */
    <T> Optional<T> getOption(ProxyOption option, Class<T> optionClass);
}
