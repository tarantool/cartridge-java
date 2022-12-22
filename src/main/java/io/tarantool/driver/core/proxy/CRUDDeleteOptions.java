package io.tarantool.driver.core.proxy;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for cluster delete operation.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDDeleteOptions extends CRUDBucketIdOptions {

    public static final String FIELDS = "fields";

    protected <O extends CRUDDeleteOptions, B extends AbstractBuilder<O, B>>
    CRUDDeleteOptions(CRUDDeleteOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDDeleteOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBucketIdOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDDeleteOptions, Builder> {

        @Override
        CRUDDeleteOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDDeleteOptions build() {
            return new CRUDDeleteOptions(this);
        }
    }
}
