package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.UpsertOptions;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpsertOptions extends ProxyBucketIdOptions<ProxyUpsertOptions> implements UpsertOptions {

    private ProxyUpsertOptions() {
    }

    /**
     * Create new instance.
     */
    public static ProxyUpsertOptions create() {
        return new ProxyUpsertOptions();
    }

    @Override
    protected ProxyUpsertOptions self() {
        return this;
    }
}
