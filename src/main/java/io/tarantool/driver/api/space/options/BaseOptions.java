package io.tarantool.driver.api.space.options;

import java.util.HashMap;
import java.util.Map;
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
    public void addOption(String option, Object value) {
        resultMap.put(option, value);
    }

    /**
     * Get an option value.
     *
     * @param option      option name
     * @param optionClass option value type
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOption(String option, Class<T> optionClass) {
        return Optional.ofNullable((T) resultMap.get(option));
    }
}
