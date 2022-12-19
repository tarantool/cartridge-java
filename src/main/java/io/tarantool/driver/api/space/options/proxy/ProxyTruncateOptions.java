package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.OperationWithTimeoutOptions;
import io.tarantool.driver.api.space.options.BaseOptions;

/**
 * Represent options for truncate cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyTruncateOptions extends BaseOptions
    implements OperationWithTimeoutOptions<ProxyTruncateOptions> {

    private ProxyTruncateOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyTruncateOptions create() {
        return new ProxyTruncateOptions();
    }

    @Override
    public ProxyTruncateOptions self() {
        return this;
    }
}
