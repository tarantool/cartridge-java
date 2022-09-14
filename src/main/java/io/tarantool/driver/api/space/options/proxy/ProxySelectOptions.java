package io.tarantool.driver.api.space.options.proxy;

import java.util.Optional;

import io.tarantool.driver.api.space.options.SelectOptions;

/**
 * Represent options for select cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxySelectOptions extends ProxyBaseOptions<ProxySelectOptions> implements SelectOptions {

    public static final String BATCH_SIZE = "batch_size";

    private ProxySelectOptions() {
    }

    public static ProxySelectOptions create() {
        return new ProxySelectOptions();
    }

    /**
     * Specifies internal batch size for transferring data from storage nodes to router nodes.
     *
     * @param batchSize batch size, should be greater than 0
     * @return this options instance
     */
    public ProxySelectOptions withBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size should be greater than 0");
        }
        addOption(BATCH_SIZE, batchSize);
        return self();
    }

    @Override
    protected ProxySelectOptions self() {
        return this;
    }

    @Override
    public Optional<Integer> getBatchSize() {
        return getOption(BATCH_SIZE, Integer.class);
    }
}
