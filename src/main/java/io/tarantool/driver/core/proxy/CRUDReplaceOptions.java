package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.crud.ProxyOption;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for cluster replace operation.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDReplaceOptions extends CRUDBucketIdOptions {

    protected <O extends CRUDReplaceOptions, B extends AbstractBuilder<O, B>>
    CRUDReplaceOptions(CRUDReplaceOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(ProxyOption.FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDReplaceOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBucketIdOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDReplaceOptions, Builder> {

        @Override
        CRUDReplaceOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDReplaceOptions build() {
            return new CRUDReplaceOptions(this);
        }
    }
}
