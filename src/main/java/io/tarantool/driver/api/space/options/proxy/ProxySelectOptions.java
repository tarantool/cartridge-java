package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.BaseOptions;
import io.tarantool.driver.api.space.options.interfaces.SelectOptions;
import io.tarantool.driver.api.space.options.enums.ProxyOption;

import java.util.Optional;

/**
 * Represent options for select cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxySelectOptions extends BaseOptions implements SelectOptions<ProxySelectOptions> {

    private ProxySelectOptions() {
    }

    /**
     * Create new instance.
     *
     * @return new options instance
     */
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
        addOption(ProxyOption.BATCH_SIZE, batchSize);
        return self();
    }

    @Override
    public ProxySelectOptions self() {
        return this;
    }

    @Override
    public Optional<Integer> getBatchSize() {
        return getOption(ProxyOption.BATCH_SIZE, Integer.class);
    }
}
