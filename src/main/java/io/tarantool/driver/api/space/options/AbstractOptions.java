package io.tarantool.driver.api.space.options;

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
public abstract class AbstractOptions<B extends AbstractOptions<B>> implements Options {

    private final Map<String, Object> resultMap = new HashMap<>();

    protected abstract B self();

    public void addOption(String option, Object value) {
        resultMap.put(option, value);
    }

    public Map<String, Object> asMap() {
        return resultMap;
    }
}
