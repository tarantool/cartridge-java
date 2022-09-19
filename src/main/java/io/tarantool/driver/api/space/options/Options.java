package io.tarantool.driver.api.space.options;

import java.util.Map;

/**
 * Marker interface for space operations options
 *
 * @author Artyom Dubinin
 * @author Alexey Kuzin
 */
public interface Options {

    /**
     * Add named option
     *
     * @param option name of option
     * @param value  value of option
     */
    void addOption(String option, Object value);

    /**
     * Return serializable options representation.
     *
     * @return a map
     */
    Map<String, Object> asMap();
}
