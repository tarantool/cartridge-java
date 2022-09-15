package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for insert cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyInsertOptions extends ProxyBaseOptions<ProxyInsertOptions> {

    private ProxyInsertOptions() {
    }

    public static ProxyInsertOptions create() {
        return new ProxyInsertOptions();
    }

    @Override
    protected ProxyInsertOptions self() {
        return this;
    }
}
