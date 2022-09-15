package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpdateOptions extends ProxyBaseOptions<ProxyUpdateOptions> {

    private ProxyUpdateOptions() {
    }

    public static ProxyUpdateOptions create() {
        return new ProxyUpdateOptions();
    }

    @Override
    protected ProxyUpdateOptions self() {
        return this;
    }
}
