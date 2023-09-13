package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.ProxyOption;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for cluster insert operation.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDInsertOptions extends CRUDBucketIdOptions {

    protected <O extends CRUDInsertOptions, B extends AbstractBuilder<O, B>>
    CRUDInsertOptions(CRUDInsertOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(ProxyOption.FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDInsertOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBucketIdOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDInsertOptions, Builder> {

        @Override
        CRUDInsertOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDInsertOptions build() {
            return new CRUDInsertOptions(this);
        }
    }
}
