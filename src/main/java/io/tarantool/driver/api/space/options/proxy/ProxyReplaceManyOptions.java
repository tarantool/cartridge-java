package io.tarantool.driver.api.space.options.proxy;

import java.util.Optional;

import io.tarantool.driver.api.space.options.ReplaceManyOptions;

/**
 * Represent options for replace_many cluster proxy operation
 *
 * @author Alexey Kuzin
 */
public final class ProxyReplaceManyOptions extends ProxyBaseOptions<ProxyReplaceManyOptions>
    implements ReplaceManyOptions {

    public static final String ROLLBACK_ON_ERROR = "rollback_on_error";
    public static final String STOP_ON_ERROR = "stop_on_error";

    private ProxyReplaceManyOptions() {
    }

    /**
     * Create new instance.
     */
    public static ProxyReplaceManyOptions create() {
        return new ProxyReplaceManyOptions();
    }

    /**
     * Specifies whether to not save any changes in the space if any tuple replace operation
     * is unsuccesful. Default value is <code>true</code>.
     *
     * @param rollbackOnError should rollback batch on error
     * @return this options instance
     */
    public ProxyReplaceManyOptions withRollbackOnError(boolean rollbackOnError) {
        addOption(ROLLBACK_ON_ERROR, rollbackOnError);
        return self();
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
    protected ProxyReplaceManyOptions self() {
        return this;
    }

    @Override
    public Optional<Boolean> getRollbackOnError() {
        return getOption(ROLLBACK_ON_ERROR, Boolean.class);
    }

    @Override
    public Optional<Boolean> getStopOnError() {
        return getOption(STOP_ON_ERROR, Boolean.class);
    }
}
