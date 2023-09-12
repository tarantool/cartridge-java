package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.crud.ProxyOption;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for cluster update operation.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDUpdateOptions extends CRUDBucketIdOptions {

    protected <O extends CRUDUpdateOptions, B extends AbstractBuilder<O, B>>
    CRUDUpdateOptions(CRUDUpdateOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(ProxyOption.FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDUpdateOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBucketIdOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDUpdateOptions, Builder> {

        @Override
        CRUDUpdateOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDUpdateOptions build() {
            return new CRUDUpdateOptions(this);
        }
    }
}
