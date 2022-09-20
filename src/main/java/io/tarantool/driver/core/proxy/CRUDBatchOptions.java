package io.tarantool.driver.core.proxy;

import java.util.Optional;

/**
 * This class is not part of the public API.
 *
 * Represent options for proxy cluster batch operations
 *
 * @author Alexey Kuzin
 */
public final class CRUDBatchOptions extends CRUDBaseOptions {

    public static final String BATCH_STOP_ON_ERROR = "stop_on_error";
    public static final String BATCH_ROLLBACK_ON_ERROR = "rollback_on_error";

    protected
    <T extends AbstractBuilder<T>>
    CRUDBatchOptions(AbstractBuilder<T> builder) {
        super(builder);

        addOption(BATCH_STOP_ON_ERROR, builder.stopOnError);
        addOption(BATCH_ROLLBACK_ON_ERROR, builder.rollbackOnError);
    }

    protected abstract static
    class AbstractBuilder<T extends AbstractBuilder<T>>
        extends CRUDBaseOptions.AbstractBuilder<CRUDBatchOptions, T> {
        private Optional<Boolean> stopOnError = Optional.empty();
        private Optional<Boolean> rollbackOnError = Optional.empty();

        public T withStopOnError(Optional<Boolean> stopOnError) {
            this.stopOnError = stopOnError;
            return self();
        }

        public T withRollbackOnError(Optional<Boolean> rollbackOnError) {
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
