package io.tarantool.driver.api.space.options;

/**
 * Represent options for insert_many cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyInsertManyOptions extends BaseOptions
    implements InsertManyOptions<ProxyInsertManyOptions> {

    private ProxyInsertManyOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
    public static ProxyInsertManyOptions create() {
        return new ProxyInsertManyOptions();
    }

    @Override
    public ProxyInsertManyOptions self() {
        return this;
    }
}
