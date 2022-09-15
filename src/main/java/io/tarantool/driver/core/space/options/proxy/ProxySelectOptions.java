package io.tarantool.driver.core.space.options.proxy;

/**
 * Represent options for select cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxySelectOptions extends ProxyBaseOptions<ProxySelectOptions> {

    public static final String BATCH_SIZE = "batch_size";

    private ProxySelectOptions() {
    }

    public static ProxySelectOptions create() {
        return new ProxySelectOptions();
    }

    public ProxySelectOptions withBatchSize(long batchSize) {
        addOption(BATCH_SIZE, batchSize);
        return self();
    }

    @Override
    protected ProxySelectOptions self() {
        return this;
    }
}
