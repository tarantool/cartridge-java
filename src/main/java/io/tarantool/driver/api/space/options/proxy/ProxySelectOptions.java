package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.interfaces.SelectOptions;

/**
 * Represent options for select cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxySelectOptions extends BaseOptions implements SelectOptions<ProxySelectOptions> {

    private ProxySelectOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxySelectOptions create() {
        return new ProxySelectOptions();
    }

    @Override
    public ProxySelectOptions self() {
        return this;
    }
}
