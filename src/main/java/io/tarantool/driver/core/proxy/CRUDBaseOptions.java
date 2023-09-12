package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.crud.ProxyOption;

import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent basic options for all cluster operations
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDBaseOptions extends CRUDAbstractOperationOptions {

    protected <O extends CRUDBaseOptions, T extends AbstractBuilder<O, T>>
    CRUDBaseOptions(AbstractBuilder<O, T> builder) {
        addOption(ProxyOption.TIMEOUT, builder.timeout);
    }

    /**
     * Inheritable Builder for basic cluster proxy operation options.
     *
     * @see CRUDAbstractOperationOptions.AbstractBuilder
     */
    protected abstract static class AbstractBuilder<O extends CRUDBaseOptions, B extends AbstractBuilder<O, B>>
        extends CRUDAbstractOperationOptions.AbstractBuilder<O, B> {
        protected Optional<Integer> timeout = Optional.empty();

        public B withTimeout(Optional<Integer> timeout) {
            this.timeout = timeout;
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
