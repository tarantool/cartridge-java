package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

import java.util.Map;
import java.util.Optional;

/**
 * Marker interface for space operations options.
 * <p> TODO: replace ProxyOption to general interface to have ability to use it not only in ProxyClient:
 * <a href="https://github.com/tarantool/cartridge-java/issues/424">issue</a> </p>
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

    Map<String, Object> asMap();
}
