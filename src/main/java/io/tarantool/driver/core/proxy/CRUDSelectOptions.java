package io.tarantool.driver.core.proxy;

import io.tarantool.driver.protocol.Packable;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for select cluster proxy operation
 *
 * @author Sergey Volgin
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
final class CRUDSelectOptions extends CRUDBucketIdOptions {

    public static final String SELECT_LIMIT = "first";
    public static final String SELECT_AFTER = "after";
    public static final String SELECT_BATCH_SIZE = "batch_size";
    public static final String FIELDS = "fields";
    public static final String MODE = "mode";

    private <B extends AbstractBuilder<B>> CRUDSelectOptions(AbstractBuilder<B> builder) {
        super(builder);

        addOption(SELECT_LIMIT, builder.selectLimit);
        addOption(SELECT_AFTER, builder.after);
        addOption(SELECT_BATCH_SIZE, builder.selectBatchSize);
        addOption(FIELDS, builder.fields);
        addOption(MODE, builder.mode);
    }

    /**
     * Inheritable Builder for select cluster proxy operation options.
     *
     * @see CRUDAbstractOperationOptions.AbstractBuilder
     */
    protected abstract static class AbstractBuilder<B extends AbstractBuilder<B>>
        extends CRUDBucketIdOptions.AbstractBuilder<CRUDSelectOptions, B> {
        private Optional<Long> selectLimit = Optional.empty();
        private Optional<Packable> after = Optional.empty();
        private Optional<Integer> selectBatchSize = Optional.empty();
        private Optional<List> fields = Optional.empty();
        private Optional<String> mode = Optional.empty();

        public B withSelectLimit(Optional<Long> selectLimit) {
            this.selectLimit = selectLimit;
            return self();
        }

        public B withSelectBatchSize(Optional<Integer> selectBatchSize) {
            this.selectBatchSize = selectBatchSize;
            return self();
        }

        public B withSelectAfter(Optional<Packable> startTuple) {
            this.after = startTuple;
            return self();
        }

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }

        public B withMode(Optional<String> mode) {
            this.mode = mode;
            return self();
        }
    }

    /**
     * Concrete Builder implementation for select cluster proxy operation options.
     */
    protected static final class Builder extends AbstractBuilder<Builder> {
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
