package io.tarantool.driver.core.proxy;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is not part of the public API.
 *
 * Represent basic options for cluster operations
 *
 * @author Alexey Kuzin
 */
public class CRUDBaseOperationOptions extends CRUDAbstractOperationOptions {

    public static final String TIMEOUT = "timeout";

    protected
    <O extends CRUDBaseOperationOptions, T extends AbstractBuilder<O, T>>
    CRUDBaseOperationOptions(AbstractBuilder<O, T> builder) {
        if (builder.timeout != null) {
            addOption(TIMEOUT, builder.timeout);
        }
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDBaseOperationOptions, T extends AbstractBuilder<O, T>>
        extends CRUDAbstractOperationOptions.AbstractBuilder<O, T> {
        protected Integer timeout;

        public T withTimeout(int timeout) {
            this.timeout = timeout;
            return self();
        }
    }

    protected static final class Builder
        extends AbstractBuilder<CRUDBaseOperationOptions, Builder> {

        @Override
        Builder self() {
            return this;
        }

        @Override
        public CRUDBaseOperationOptions build() {
            return new CRUDBaseOperationOptions(this);
        }
    }
}
