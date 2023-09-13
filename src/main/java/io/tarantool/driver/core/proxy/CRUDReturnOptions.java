package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.ProxyOption;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent returned options for cluster proxy operations.
 * The return set of fields of the tuples can be specified.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDReturnOptions extends CRUDBaseOptions {

    protected <O extends CRUDReturnOptions, B extends AbstractBuilder<O, B>>
    CRUDReturnOptions(CRUDReturnOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(ProxyOption.FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDReturnOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBaseOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDReturnOptions, Builder> {

        @Override
        CRUDReturnOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDReturnOptions build() {
            return new CRUDReturnOptions(this);
        }
    }
}
