package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for delete cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyDeleteOptions extends ProxyBaseOptions<ProxyDeleteOptions> {

    private ProxyDeleteOptions() {
    }

    public static ProxyDeleteOptions create() {
        return new ProxyDeleteOptions();
    }

    @Override
    protected ProxyDeleteOptions self() {
        return this;
    }
}
