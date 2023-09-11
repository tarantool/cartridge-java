package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.ReplaceManyOptions;

import java.util.Optional;

/**
 * Represent options for replace_many cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyReplaceManyOptions extends BaseOptions
    implements ReplaceManyOptions<ProxyReplaceManyOptions> {

    public static final String STOP_ON_ERROR = "stop_on_error";

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

    /**
     * Specifies whether to not try to replace more tuples into the space if any tuple replace
     * operation is unsuccesful. Default value is <code>true</code>.
     *
     * @param stopOnError should stop batch on error
     * @return this options instance
     */
    public ProxyReplaceManyOptions withStopOnError(boolean stopOnError) {
        addOption(STOP_ON_ERROR, stopOnError);
        return self();
    }

    @Override
    public ProxyReplaceManyOptions self() {
        return this;
    }

    @Override
    public Optional<Boolean> getStopOnError() {
        return getOption(STOP_ON_ERROR, Boolean.class);
    }
}
