package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.InsertManyOptions;

import java.util.Optional;

/**
 * Represent options for insert_many cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyInsertManyOptions extends BaseOptions
    implements InsertManyOptions<ProxyInsertManyOptions> {

    public static final String STOP_ON_ERROR = "stop_on_error";

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

    /**
     * Specifies whether to not try to insert more tuples into the space if any tuple insert
     * operation is unsuccesful. Default value is <code>true</code>.
     *
     * @param stopOnError should stop batch on error
     * @return this options instance
     */
    public ProxyInsertManyOptions withStopOnError(boolean stopOnError) {
        addOption(STOP_ON_ERROR, stopOnError);
        return self();
    }

    @Override
    public ProxyInsertManyOptions self() {
        return this;
    }

    @Override
    public Optional<Boolean> getStopOnError() {
        return getOption(STOP_ON_ERROR, Boolean.class);
    }
}
