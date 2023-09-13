package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.interfaces.UpdateOptions;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpdateOptions extends BaseOptions implements UpdateOptions<ProxyUpdateOptions> {

    private ProxyUpdateOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyUpdateOptions create() {
        return new ProxyUpdateOptions();
    }

    @Override
    public ProxyUpdateOptions self() {
        return this;
    }
}
