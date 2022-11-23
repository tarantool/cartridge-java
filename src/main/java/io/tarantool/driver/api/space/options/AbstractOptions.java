package io.tarantool.driver.api.space.options;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * An abstract class-container for all operation options.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public abstract class AbstractOptions<B extends AbstractOptions<B>> implements Options {

    private final Map<String, Object> resultMap = new HashMap<>();

    protected abstract B self();

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
