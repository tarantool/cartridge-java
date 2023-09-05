package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable mode.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithModeOptions<T extends OperationWithModeOptions<T>> extends Options, Self<T> {
    String MODE = "mode";

    /**
     * Specifies the mode for operations (select, count, get) on a specific node type (mode == "write" - master, mode
     * == "read" - replica). By default, mode is "read".
     *
     * @param mode mode for operations (select, get, count).
     * @return this options instance.
     */
    default T withMode(String mode) {
        if (!mode.equals("read") && !mode.equals("write")) {
            throw new IllegalArgumentException("Mode should be \"read\" or \"write\"");
        }

        addOption(MODE, mode);
        return self();
    }

    /**
     * Return operation mode.
     *
     * @return mode.
     */
    default Optional<String> getMode() {
        return getOption(MODE, String.class);
    }

}
