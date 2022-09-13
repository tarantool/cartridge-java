package io.tarantool.driver.core.proxy;

import java.util.Map;

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
    <O extends CRUDBatchOptions, T extends AbstractBuilder<O, T>>
    CRUDBatchOptions(AbstractBuilder<O, T> builder) {
        super(builder);

        if (builder.stopOnError != null) {
            addOption(BATCH_STOP_ON_ERROR, builder.stopOnError);
        }

        if (builder.rollbackOnError != null) {
            addOption(BATCH_ROLLBACK_ON_ERROR, builder.rollbackOnError);
        }
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDBaseOptions, T extends AbstractBuilder<O, T>>
        extends CRUDBaseOptions.AbstractBuilder<O, T> {
        private Boolean stopOnError;
        private Boolean rollbackOnError;

        public T withStopOnError(Boolean stopOnError) {
            this.stopOnError = stopOnError;
            return self();
        }

        public T withRollbackOnError(Boolean rollbackOnError) {
            this.rollbackOnError = rollbackOnError;
            return self();
        }
    }

    protected static final class Builder
        extends AbstractBuilder<CRUDBatchOptions, Builder> {

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
