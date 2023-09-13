package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.ProxyOption;
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

    private <B extends AbstractBuilder<B>> CRUDSelectOptions(AbstractBuilder<B> builder) {
        super(builder);

        addOption(ProxyOption.FIRST, builder.first);
        addOption(ProxyOption.AFTER, builder.after);
        addOption(ProxyOption.BATCH_SIZE, builder.batchSize);
        addOption(ProxyOption.FIELDS, builder.fields);
        addOption(ProxyOption.MODE, builder.mode);
    }

    /**
     * Inheritable Builder for select cluster proxy operation options.
     *
     * @see CRUDAbstractOperationOptions.AbstractBuilder
     */
    protected abstract static class AbstractBuilder<B extends AbstractBuilder<B>>
        extends CRUDBucketIdOptions.AbstractBuilder<CRUDSelectOptions, B> {
        private Optional<Long> first = Optional.empty();
        private Optional<Packable> after = Optional.empty();
        private Optional<Integer> batchSize = Optional.empty();
        private Optional<List> fields = Optional.empty();
        private Optional<String> mode = Optional.empty();

        public B withSelectLimit(Optional<Long> first) {
            this.first = first;
            return self();
        }

        public B withSelectBatchSize(Optional<Integer> batchSize) {
            this.batchSize = batchSize;
            return self();
        }

        public B withSelectAfter(Optional<Packable> after) {
            this.after = after;
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
