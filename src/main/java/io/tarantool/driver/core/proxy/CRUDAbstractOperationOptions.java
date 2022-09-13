package io.tarantool.driver.core.proxy;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * An abstract class necessary for implementing CRT
 * (curiously recurring template) pattern.
 *
 * @author Alexey Kuzin
 */
abstract class CRUDAbstractOperationOptions {

    private final Map<String, Object> resultMap = new HashMap<>();

    protected abstract static
    class AbstractBuilder<O extends CRUDAbstractOperationOptions, B extends AbstractBuilder<O, B>> {
        abstract B self();

        public abstract O build();
    }

    protected void addOption(String option, Object value) {
        resultMap.put(option, value);
    }

    public Map<String, Object> asMap() {
        return resultMap;
    }
}
