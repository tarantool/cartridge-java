package io.tarantool.driver.api.space.options;

/**
 * Represent options for delete cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyDeleteOptions extends BaseOptions
    implements DeleteOptions<ProxyDeleteOptions> {

    private ProxyDeleteOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyDeleteOptions create() {
        return new ProxyDeleteOptions();
    }

    @Override
    public ProxyDeleteOptions self() {
        return this;
    }
}
