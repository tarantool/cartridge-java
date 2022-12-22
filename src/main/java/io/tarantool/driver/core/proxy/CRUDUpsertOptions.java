package io.tarantool.driver.core.proxy;

import java.util.List;
import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent options for cluster upsert operation.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDUpsertOptions extends CRUDBucketIdOptions {

    public static final String FIELDS = "fields";

    protected <O extends CRUDUpsertOptions, B extends AbstractBuilder<O, B>>
    CRUDUpsertOptions(CRUDUpsertOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(FIELDS, builder.fields);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDUpsertOptions, B extends AbstractBuilder<O, B>>
            extends CRUDBucketIdOptions.AbstractBuilder<O, B> {
        private Optional<List> fields = Optional.empty();

        public B withFields(Optional<List> fields) {
            this.fields = fields;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDUpsertOptions, Builder> {

        @Override
        CRUDUpsertOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDUpsertOptions build() {
            return new CRUDUpsertOptions(this);
        }
    }
}
