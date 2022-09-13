package io.tarantool.driver.core.proxy;

import io.tarantool.driver.protocol.Packable;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * Represent options for proxy cluster select operations
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public final class CRUDSelectOperationOptions extends CRUDBaseOperationOptions {

    public static final String SELECT_LIMIT = "first";
    public static final String SELECT_AFTER = "after";
    public static final String SELECT_BATCH_SIZE = "batch_size";

    protected
    <O extends CRUDSelectOperationOptions, T extends AbstractBuilder<O, T>>
    CRUDSelectOperationOptions(AbstractBuilder<O, T> builder) {
        super(builder);

        if (builder.selectLimit != null) {
            addOption(SELECT_LIMIT, builder.selectLimit);
        }

        if (builder.after != null) {
            addOption(SELECT_AFTER, builder.after);
        }

        if (builder.selectBatchSize != null) {
            addOption(SELECT_BATCH_SIZE, builder.selectBatchSize);
        }
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDSelectOperationOptions, T extends AbstractBuilder<O, T>>
        extends CRUDBaseOperationOptions.AbstractBuilder<O, T> {
        private Long selectLimit;
        private Packable after;
        private Long selectBatchSize;

        public T withSelectLimit(long selectLimit) {
            this.selectLimit = selectLimit;
            return self();
        }

        public T withSelectBatchSize(long selectBatchSize) {
            this.selectBatchSize = selectBatchSize;
            return self();
        }

        public T withSelectAfter(Packable startTuple) {
            this.after = startTuple;
            return self();
        }
    }

    protected static final class Builder
        extends AbstractBuilder<CRUDSelectOperationOptions, Builder> {

        @Override
        Builder self() {
            return this;
        }

        @Override
        public CRUDSelectOperationOptions build() {
            return new CRUDSelectOperationOptions(this);
        }
    }
}
