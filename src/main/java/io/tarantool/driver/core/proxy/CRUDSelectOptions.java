package io.tarantool.driver.core.proxy;

import io.tarantool.driver.protocol.Packable;

/**
 * This class is not part of the public API.
 *
 * Represent options for select cluster proxy operations
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 */
public final class CRUDSelectOptions extends CRUDBaseOperationOptions {

    public static final String SELECT_LIMIT = "first";
    public static final String SELECT_AFTER = "after";
    public static final String SELECT_BATCH_SIZE = "batch_size";

    protected
    <O extends CRUDSelectOptions, T extends AbstractBuilder<O, T>>
    CRUDSelectOptions(AbstractBuilder<O, T> builder) {
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

    /**
     * Inheritable Builder for select cluster proxy operation options.
     *
     * @see CRUDAbstractOperationOptions.AbstractBuilder
     */
    protected abstract static
    class AbstractBuilder<O extends CRUDSelectOptions, T extends AbstractBuilder<O, T>>
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

    /**
     * Concrete Builder implementation for select cluster proxy operation options.
     */
    protected static final class Builder
        extends AbstractBuilder<CRUDSelectOptions, Builder> {

        @Override
        Builder self() {
            return this;
        }

        @Override
        public CRUDSelectOptions build() {
            return new CRUDSelectOptions(this);
        }
    }
}
