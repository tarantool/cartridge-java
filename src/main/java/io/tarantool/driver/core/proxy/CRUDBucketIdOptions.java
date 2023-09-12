package io.tarantool.driver.core.proxy;

import io.tarantool.driver.api.space.options.enums.crud.ProxyOption;

import java.util.Optional;

/**
 * This class is not part of the public API.
 * <p>
 * Represent bucket id options for cluster proxy operations.
 * Options for functions that uses bucket id to find storage location.
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
class CRUDBucketIdOptions extends CRUDBaseOptions {

    protected <O extends CRUDBucketIdOptions, B extends AbstractBuilder<O, B>>
    CRUDBucketIdOptions(CRUDBucketIdOptions.AbstractBuilder<O, B> builder) {
        super(builder);
        addOption(ProxyOption.BUCKET_ID, builder.bucketId);
    }

    protected abstract static
    class AbstractBuilder<O extends CRUDBucketIdOptions, B extends AbstractBuilder<O, B>>
        extends CRUDBaseOptions.AbstractBuilder<O, B> {
        private Optional<Integer> bucketId = Optional.empty();

        public B withBucketId(Optional<Integer> bucketId) {
            this.bucketId = bucketId;
            return self();
        }
    }

    protected static final class Builder extends AbstractBuilder<CRUDBucketIdOptions, Builder> {

        @Override
        CRUDBucketIdOptions.Builder self() {
            return this;
        }

        @Override
        public CRUDBucketIdOptions build() {
            return new CRUDBucketIdOptions(this);
        }
    }
}
