package io.tarantool.driver.api.space.options.proxy;

/**
 * Represent options for truncate cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyTruncateOptions extends ProxyBaseOptions<ProxyTruncateOptions> {

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
    protected ProxyTruncateOptions self() {
        return this;
    }
}
