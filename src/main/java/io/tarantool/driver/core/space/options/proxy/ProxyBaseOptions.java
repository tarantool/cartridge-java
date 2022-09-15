package io.tarantool.driver.core.space.options.proxy;

import io.tarantool.driver.core.space.options.Options;

/**
 * Represent options for all proxy functions
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
abstract class ProxyBaseOptions<B extends ProxyBaseOptions<B>> extends Options<B> {

    public static final String TIMEOUT = "timeout";

    public B withTimeout(int timeout) {
        addOption(TIMEOUT, timeout);
        return self();
    }
}
