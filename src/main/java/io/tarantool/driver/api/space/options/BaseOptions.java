package io.tarantool.driver.api.space.options;

import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Base functional for all operation options.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public abstract class BaseOptions implements Options {

    private final Map<String, Object> resultMap = new HashMap<>();

    /**
     * Add an option value.
     *
     * @param option option name
     * @param value  option value
     */
    public void addOption(ProxyOption option, Object value) {
        if (Objects.nonNull(value)) {
            resultMap.put(option.toString(), value);
        }
    }

    /**
     * Get an option value.
     *
     * @param option      option name
     * @param optionClass option value type
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOption(ProxyOption option, Class<T> optionClass) {
        return Optional.ofNullable((T) resultMap.get(option.toString()));
    }

    public Map<String, Object> asMap() {
        return resultMap;
    }
}
