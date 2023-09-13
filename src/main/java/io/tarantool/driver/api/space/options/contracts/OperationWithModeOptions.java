package io.tarantool.driver.api.space.options.contracts;

import io.tarantool.driver.api.space.options.enums.Mode;
import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.Options;
import io.tarantool.driver.api.space.options.interfaces.Self;

import java.util.Optional;

/**
 * Base interface for all operation options that may have a configurable mode.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithModeOptions<T extends OperationWithModeOptions<T>> extends Options, Self<T> {

    /**
     * Specifies the mode for operations (select, count, get) on a specific node type (mode == "write" - master, mode
     * == "read" - replica). By default, mode is "read".
     *
     * @param mode mode for operations (select, get, count).
     * @return this options instance.
     */
    default T withMode(Mode mode) {
        addOption(ProxyOption.MODE, mode.value());
        return self();
    }

    /**
     * Return operation mode.
     *
     * @return mode.
     */
    default Optional<Mode> getMode() {
        return getOption(ProxyOption.MODE, Mode.class);
    }

}
