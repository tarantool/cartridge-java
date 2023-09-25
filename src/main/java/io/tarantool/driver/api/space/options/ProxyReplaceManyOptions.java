package io.tarantool.driver.api.space.options;

/**
 * Represent options for replace_many cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyReplaceManyOptions extends BaseOptions
    implements ReplaceManyOptions<ProxyReplaceManyOptions> {

    private ProxyReplaceManyOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyReplaceManyOptions create() {
        return new ProxyReplaceManyOptions();
    }

    @Override
    public ProxyReplaceManyOptions self() {
        return this;
    }
}
