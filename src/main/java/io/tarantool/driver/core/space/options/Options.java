package io.tarantool.driver.core.space.options;

import java.util.HashMap;
import java.util.Map;

/**
 * Public API for space operations.
 *
 * An abstract class necessary for implementing CRT (curiously recurring template)
 * pattern for the cluster proxy operation options.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public abstract class Options<B extends Options<B>> {

    private final Map<String, Object> resultMap = new HashMap<>();

    protected abstract B self();

    protected void addOption(String option, Object value) {
        resultMap.put(option, value);
    }
    /**
     * Return serializable options representation.
     *
     * @return a map
     */
    public Map<String, Object> asMap() {
        return resultMap;
    }
}
