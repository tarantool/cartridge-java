package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for replace cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyReplaceOptions extends ProxyBaseOptions<ProxyReplaceOptions> {

    private ProxyReplaceOptions() {
    }

    public static ProxyReplaceOptions create() {
        return new ProxyReplaceOptions();
    }

    @Override
    protected ProxyReplaceOptions self() {
        return this;
    }
}
