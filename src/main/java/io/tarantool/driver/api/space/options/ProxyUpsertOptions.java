package io.tarantool.driver.api.space.options;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpsertOptions extends BaseOptions implements UpsertOptions<ProxyUpsertOptions> {

    private ProxyUpsertOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyUpsertOptions create() {
        return new ProxyUpsertOptions();
    }

    @Override
    public ProxyUpsertOptions self() {
        return this;
    }
}
