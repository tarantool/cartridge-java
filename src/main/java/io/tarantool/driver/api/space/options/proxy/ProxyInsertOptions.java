package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.InsertOptions;
import io.tarantool.driver.api.space.options.BaseOptions;

/**
 * Represent options for insert cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyInsertOptions extends BaseOptions implements InsertOptions<ProxyInsertOptions> {

    private ProxyInsertOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyInsertOptions create() {
        return new ProxyInsertOptions();
    }

    @Override
    public ProxyInsertOptions self() {
        return this;
    }
}
