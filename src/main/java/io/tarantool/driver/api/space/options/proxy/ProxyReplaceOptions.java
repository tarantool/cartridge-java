package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.ReplaceOptions;

/**
 * Represent options for replace cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyReplaceOptions extends BaseOptions implements ReplaceOptions<ProxyReplaceOptions> {

    private ProxyReplaceOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyReplaceOptions create() {
        return new ProxyReplaceOptions();
    }

    @Override
    public ProxyReplaceOptions self() {
        return this;
    }
}
