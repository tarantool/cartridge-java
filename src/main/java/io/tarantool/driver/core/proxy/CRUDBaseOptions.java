package io.tarantool.driver.core.proxy;

import io.tarantool.driver.core.space.options.Options;

/**
 * This class is not part of the public API.
 *
 * Represent basic options for all cluster operations
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDBaseOptions extends CRUDAbstractOperationOptions {

    public static final String TIMEOUT = "timeout";

    protected
    <O extends CRUDBaseOptions, T extends AbstractBuilder<O, T>>
    CRUDBaseOptions(AbstractBuilder<O, T> builder) {
        if (builder.timeout != null) {
            addOption(TIMEOUT, builder.timeout);
        }

        if (builder.options != null) {
            Object batchSize = builder.options.asMap().get(TIMEOUT);
            if (batchSize != null) {
                addOption(TIMEOUT, batchSize);
            }
        }
    }

    /**
     * Inheritable Builder for basic cluster proxy operation options.
     *
     * @see CRUDAbstractOperationOptions.AbstractBuilder
     */
    protected abstract static
    class AbstractBuilder<O extends CRUDBaseOptions, T extends AbstractBuilder<O, T>>
        extends CRUDAbstractOperationOptions.AbstractBuilder<O, T> {
        protected Integer timeout;
        protected Options options;

        public T withTimeout(int timeout) {
            this.timeout = timeout;
            return self();
        }

        public T withOptions(Options options) {
            this.options = options;
            return self();
        }
    }

    /**
     * Concrete Builder implementation for basic cluster proxy operation options.
     */
    protected static final class Builder
        extends AbstractBuilder<CRUDBaseOptions, Builder> {

        @Override
        Builder self() {
            return this;
        }

        @Override
        public CRUDBaseOptions build() {
            return new CRUDBaseOptions(this);
        }
    }
}
