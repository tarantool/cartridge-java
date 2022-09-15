package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpsertOptions extends ProxyBaseOptions<ProxyUpsertOptions> {

    private ProxyUpsertOptions() {
    }

    public static ProxyUpsertOptions create() {
        return new ProxyUpsertOptions();
    }

    @Override
    protected ProxyUpsertOptions self() {
        return this;
    }
}
