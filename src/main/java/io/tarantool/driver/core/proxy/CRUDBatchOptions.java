package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.ProxyOption;

import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for proxy cluster batch operations
 *
 * @author Alexey Kuzin
 */
final class CRUDBatchOptions extends CRUDReturnOptions {

    private <T extends AbstractBuilder<T>>
    CRUDBatchOptions(AbstractBuilder<T> builder) {
        super(builder);
        addOption(ProxyOption.STOP_ON_ERROR, builder.stopOnError);
        addOption(ProxyOption.ROLLBACK_ON_ERROR, builder.rollbackOnError);
    }

    protected abstract static class AbstractBuilder<B extends AbstractBuilder<B>>
            extends CRUDReturnOptions.AbstractBuilder<CRUDBatchOptions, B> {
        private Optional<Boolean> stopOnError = Optional.empty();
        private Optional<Boolean> rollbackOnError = Optional.empty();

        public B withStopOnError(Optional<Boolean> stopOnError) {
            this.stopOnError = stopOnError;
            return self();
        }

        public B withRollbackOnError(Optional<Boolean> rollbackOnError) {
            this.rollbackOnError = rollbackOnError;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<Builder> {
        @Override
        Builder self() {
            return this;
        }

        @Override
        public CRUDBatchOptions build() {
            return new CRUDBatchOptions(this);
        }
    }
}
