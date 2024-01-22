package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

/**
 * Base interface for all operation options that may have a configurable "force_map_call" option.
 *
 * @author Belonogov Nikolay
 */
public interface OperationWithForceMapCallOptions<T extends OperationWithForceMapCallOptions<T>>
    extends Options, Self<T> {

    /**
     * Sets "force_map_call" option to true. if true then the map call is performed without any optimizations even,
     * default value is false.
     *
     * @return forceMapCall option value.
     */
    default T forceMapCall() {
        addOption(ProxyOption.FORCE_MAP_CALL, true);
        return self();
    }

    /**
     * @return "force_map_call" option value.
     */
    default Optional<Boolean> getForceMapCall() {
        return getOption(ProxyOption.FORCE_MAP_CALL, Boolean.class);
    }
}
