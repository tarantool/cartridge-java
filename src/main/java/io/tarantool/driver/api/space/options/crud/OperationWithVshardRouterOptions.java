package io.tarantool.driver.api.space.options.crud;

import java.util.Optional;

import io.tarantool.driver.api.space.options.Options;
import io.tarantool.driver.api.space.options.Self;
import io.tarantool.driver.api.space.options.crud.enums.ProxyOption;

/**
 * Base interface for all operation options that may have a configurable "vshard_router" value.
 */
public interface OperationWithVshardRouterOptions<T extends OperationWithVshardRouterOptions<T>>
    extends Options, Self<T> {

    /**
     * Sets "vshard_router" option value. Cartridge vshard group name or vshard router instance. Set this parameter
     * if your space is not a part of the default vshard cluster.
     *
     * @param vshardRouter cartridge vshard group name or vshard router instance. Set this parameter if your space
     *                     is not a part of the default vshard cluster.
     * @return this options instance.
     * @throws IllegalArgumentException if vshardRouter is null or consists only of spaces.
     */
    default T withVshardRouter(String vshardRouter) throws IllegalArgumentException {
        if (vshardRouter.trim().isEmpty()) {
            throw new IllegalArgumentException("\"vshardRouter\" parameter cannot be empty or consist only of spaces.");
        }
        addOption(ProxyOption.VSHARD_ROUTER, vshardRouter);
        return self();
    }

    /**
     * @return "vshard_router" option value.
     */
    default Optional<String> getVshardRouter() {
        return getOption(ProxyOption.VSHARD_ROUTER, String.class);
    }
}
