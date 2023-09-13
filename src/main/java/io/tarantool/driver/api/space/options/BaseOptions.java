package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.Options;

import java.util.EnumMap;
import java.util.Optional;

/**
 * Base functional for all operation options.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public abstract class BaseOptions implements Options {

    private final EnumMap<ProxyOption, Object> resultMap = new EnumMap<>(ProxyOption.class);

    /**
     * Add an option value.
     *
     * @param option option name
     * @param value  option value
     */
    public void addOption(ProxyOption option, Object value) {
        resultMap.put(option, value);
    }

    /**
     * Get an option value.
     *
     * @param option      option name
     * @param optionClass option value type
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOption(ProxyOption option, Class<T> optionClass) {
        return Optional.ofNullable((T) resultMap.get(option));
    }
}
