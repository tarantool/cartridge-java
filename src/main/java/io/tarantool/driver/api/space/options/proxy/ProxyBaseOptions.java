package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.AbstractOptions;

/**
 * Represent options for all proxy functions
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
abstract class ProxyBaseOptions<B extends ProxyBaseOptions<B>> extends AbstractOptions<B> {

    public static final String TIMEOUT = "timeout";

    public B withTimeout(int timeout) {
        addOption(TIMEOUT, timeout);
        return self();
    }
}
